from flask import render_template, request, Flask, url_for, session, redirect, abort
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import func
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.oauth import OAuth
from flask.ext.celery import Celery
from celery.task.sets import TaskSet
from sqlalchemy.dialects import postgres
from collections import defaultdict
import json
import datetime
from imdb import IMDb
import urllib2
import os
import operator

def make_app():
    return Flask("MovieFinder")

app = make_app()
app.config.from_object('settings')
db = SQLAlchemy(app)
#db.engine.echo = True
celery = Celery(app)



class Movie(db.Model):
    __tablename__ = "movies"
    imdb_id         = db.Column(db.Integer(), primary_key=True)
    imdb_string_id  = db.Column(db.String(), unique=True) # *real* IMDB id
    # Metadata
    title           = db.Column(db.String())
    year            = db.Column(db.Integer(), nullable=True)
    director        = db.Column(db.String(), nullable=True)
    genre           = db.Column(db.String(), nullable=True)
    poster_url      = db.Column(db.String(), nullable=True)

    # Scores + recommendations + date cached
    date_cached     = db.Column(db.DateTime(), nullable=True)
    tomatoes_score  = db.Column(db.Integer(), nullable=True)
    imdb_score      = db.Column(db.Float(), nullable=True)
    recomendations = db.Column(postgres.ARRAY(db.Integer), nullable=True)

    def get_poster_url(self):
        return url_for("static", filename="posters/%s/%s.jpg"%(str(self.imdb_id)[0], self.imdb_id))


class User(db.Model):
    __tablename__ = "users"
    user_id = db.Column(db.BigInteger(), primary_key=True)
    movies_liked = db.Column(postgres.ARRAY(db.Integer), default=[])
    movies_hidden = db.Column(postgres.ARRAY(db.Integer), default=[])


    def get_movies_liked(self):
        if not len(self.movies_liked):
            return []

        return Movie.query.filter(Movie.imdb_id.in_(self.movies_liked)).all()


try:
    db.create_all()
except Exception:
    pass

oauth = OAuth()

facebook = oauth.remote_app('facebook',
    base_url='https://graph.facebook.com/',
    request_token_url=None,
    access_token_url='/oauth/access_token',
    authorize_url='https://www.facebook.com/dialog/oauth',
    consumer_key=app.config["FACEBOOK_APP_ID"],
    consumer_secret=app.config["FACEBOOK_APP_SECRET"],
    request_token_params={'scope': 'email'}
)

_old_render = render_template

USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.162 Safari/535.19"

@celery.task(name="MovieFinder.GetRecommendations")
def GetRecommendations(movie_id, movie_db=None):
    if not movie_db:
        try:
            movie_db = Movie.query.filter_by(imdb_id=movie_id).one()
        except NoResultFound:
            print "Movie %s not found"%movie_id
            return

    print "Fetching recommendations"
    RECOMENDATION_URL = "http://www.imdb.com/widget/recommendations/_ajax/adapter/shoveler?start=1&count=25&specs=p13nsims:tt"+str(movie_db.imdb_string_id)
    req = urllib2.Request(RECOMENDATION_URL, headers={"User-Agent":USER_AGENT, "Host":"www.imdb.com"})
    try:
        data = json.loads(urllib2.urlopen(req).read())
    except Exception,e:
        print "Error getting recommendations: %s"%e
    else:
        if not data["status"] == 200:
            print "Error: status code %s"%data["status"]
        else:
            ids_to_get = [obj["display"]["titleId"].lstrip("tt") for obj in data["model"]["items"]]
            ids_that_exist = set([x[0] for x in db.session.query(Movie.imdb_string_id).filter(Movie.imdb_id.in_(ids_to_get))])
            ids_that_dont_exist = set(ids_to_get) - ids_that_exist
            job = TaskSet(tasks=[
                AddMovie.subtask((id, False)) for id in ids_that_dont_exist
            ])
            job.apply_async()
            print "Dispatched %s jobs"%len(ids_that_dont_exist)

            movie_db.recomendations = [int(x) for x in ids_to_get]

            db.session.add(movie_db)
            db.session.commit()


@celery.task(name="MovieFinder.AddMovie")
def AddMovie(movie_id, get_recommendations=True):
    print "Processing movie %s"%movie_id

    string_id = movie_id
    movie_id = int(movie_id) # Truncates the leading 0's :(

    try:
        movie_db = Movie.query.filter_by(imdb_id=movie_id).one()
    except NoResultFound:
        print "Movie %s not found"%movie_id
        movie_db = Movie()
        movie_db.imdb_id = movie_id
        movie_db.imdb_string_id = string_id

    imdb = IMDb()
    movie = imdb.get_movie(movie_id)
    if not movie.get("title"):
        print "Movie %s not in IMDB apparently"%movie_id
        return

    movie_db.title = movie.get("title")
    movie_db.year = movie.get("year")

    if movie.get("director"):
        # use the 1st one
        movie_db.director = movie.get("director")[0].get("name")

    movie_db.imdb_score = movie.get("rating")
    movie_db.date_cached = datetime.datetime.now()

    if "genre" in movie.keys():
        movie_db.genre = ",".join(movie.get("genre"))

    db.session.add(movie_db)
    try:
        db.session.commit()
    except Exception,e:
        print "Error adding ID %s: %s"%(movie_id,e)
        db.session.rollback()
    else:
        print "Handled ID %s"%movie_id
        print "Rating: %s"%movie_db.imdb_score


    if "cover url" in movie.keys():
        try:
            folder_path = os.path.join("static","posters",str(movie_id)[0])
            if not os.path.isdir(folder_path):
                os.makedirs(folder_path)
            _path = os.path.join(folder_path,"%s.jpg"%movie_id)

            if not os.path.exists(_path):
                print "Downloading poster to path %s"%_path
                with open(_path,"wb") as poster:
                    poster_req = urllib2.Request(movie.get("cover url"))
                    wr = urllib2.urlopen(poster_req)
                    while True:
                        d = wr.read(100)
                        if d == "":
                            break
                        poster.write(d)
                print "Poster downloaded"
                movie_db.poster_url = _path
                db.session.add(movie_db)
                try:
                    db.session.commit()
                except Exception,e:
                    print "Could not set poster_url: %s"%e
                    db.session.rollback()
        except Exception,e:
            print "Error downloading poster: %s"%e

    if get_recommendations:
        GetRecommendations(movie_id, movie_db)

    db.session.close()

def render_template(*args, **kwargs):
    kwargs["user"] = get_user()
    return _old_render(*args, **kwargs)

@app.route("/")
def index():
    user = get_user()

    if not user:
        return render_template("connect.html")

    return render_template("index.html", placeholder=random_movie())


@app.route("/api/randommovie")
def random_movie():
    try:
        return db.session.query(Movie.title).order_by(func.random()).limit(1).one()[0]
    except NoResultFound:
        return ""

@app.route("/api/getrecommendations")
def recommendations():

    import time
    t1 = time.time()

    user = get_user()

    if not user:
        return abort(400)

    user_likes = user.movies_liked
    shit_they_like = db.session.query(Movie.recomendations).filter(Movie.imdb_id.in_(user_likes))\
                                                           .filter(Movie.recomendations != None)\
                                                           .filter(Movie.recomendations != []).limit(150).all()


    id_counters = defaultdict(int)

    for item in shit_they_like:
        for id in item[0]:
            if not id in user.movies_hidden and not id in user.movies_liked:
                id_counters[id]+=1

    sorted_stuff = sorted(id_counters.iteritems(), key=operator.itemgetter(1))
    item_ids = sorted_stuff[-10:]
    items = db.session.query(Movie).filter(Movie.imdb_id.in_([x[0] for x in item_ids])).all()

    time_taken = time.time() - t1
    print "Time taken to process recommendations: %s"%time_taken

    return json.dumps([{"id":i.imdb_id,"poster":i.get_poster_url(),
                        "title":i.title,"year":i.year,
                        "director":i.director, "score":i.imdb_score,
                        "imdb_id":i.imdb_string_id
                        } for i in items])


@app.route("/api/recommendation", methods=["PUT"])
def recommendation():
    user = get_user()
    if not user:
        return abort(400)

    try:
        data = json.loads(request.data)
    except Exception:
        return abort(400)
    print user.movies_hidden
    if data["hidden"]:
        print "hidden shit"
        if not int(data["id"]) in user.movies_hidden:
            print "hiding..."
            db.session.query(User).filter(User.user_id == user.user_id).update(
                    {User.movies_hidden:User.movies_hidden.op("+")([int(data["id"])])}, synchronize_session=False
            )
            db.session.commit()

    return "success"

@app.route("/api/movies")
def getMovies():
    user = get_user()
    if not user:
        return abort(400)
    return json.dumps([{"id":x.imdb_id, "title":x.title} for x in user.get_movies_liked()])

@app.route("/api/movies/<id>", methods=["PUT","DELETE"])
def userMovie(id):
    user = get_user()
    if not user:
        return abort(400)

    if request.method == "DELETE":

        id = int(id.lstrip("tt"))
        if id in user.movies_liked:
            db.session.query(User).filter(User.user_id == user.user_id).update(
                    {User.movies_liked:User.movies_liked.op("-")(id)}, synchronize_session=False
            )
            db.session.commit()
        return "done"

    try:
        data = json.loads(request.data)
    except Exception:
        return abort(400)

    id = data["id"]
    if id.startswith("tt"):
        string_id = id[2:]
        id = int(id[2:])
    else:
        string_id = id
        id = int(id)

    if not db.session.query(Movie).filter_by(imdb_id=id).count():
        movie = Movie()
        movie.title = data["title"]
        movie.imdb_id = id
        movie.imdb_string_id = string_id
        db.session.add(movie)
        try:
            db.session.commit()
        except Exception,e:
            print "Error adding new movie: %s"%e
            db.session.rollback()
        else:
            AddMovie.apply_async((string_id,))
    else:
        GetRecommendations.apply_async((string_id,))

    if not id in user.movies_liked:
        db.session.query(User).filter(User.user_id == user.user_id).update(
                {User.movies_liked:User.movies_liked.op("+")([id])}, synchronize_session=False
        )

        db.session.commit()

    return "success"


@app.route("/login")
def login():
    return facebook.authorize(callback=url_for('facebook_authorized',
        next=request.args.get('next') or request.referrer or
             None,
        _external=True))

@app.route('/login/authorized')
def facebook_authorized():
    session['oauth_token'] = (request.args.get('access_token',''), '')
    me = facebook.get('/me')

    if not db.session.query(User).filter_by(user_id=me.data["id"]).count():
        # Make a user
        u = User()
        u.user_id = me.data["id"]
        db.session.add(u)
        db.session.commit()
        print "Created a user account for ID %s"%me.data["id"]

    session["auth_user"] = me.data["id"]

    return redirect(url_for("index"))


@facebook.tokengetter
def get_facebook_oauth_token():
    return session.get('oauth_token')

def get_user():
    uid = session.get("auth_user",None)
    if not uid:
        return None
    try:
        return db.session.query(User).filter_by(user_id=uid).one()
    except NoResultFound:
        session.clear()
        return None


if __name__ == "__main__":

    import signal, sys
    def exit_handler(signum, frame):
        sys.exit(0)
    signal.signal(signal.SIGTERM, exit_handler)

    app.run()