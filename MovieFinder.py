from flask import render_template, request, Flask, url_for, session, redirect, abort
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import ProgrammingError, ResourceClosedError
from sqlalchemy.sql import func
from sqlalchemy.dialects import postgres
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.oauth import OAuth
from flask.ext.celery import Celery
from celery.task.sets import TaskSet
from collections import defaultdict
from sqlalchemy import not_, and_
import json
import datetime
import urllib2
import os
import operator
from imdb import IMDb, IMDbDataAccessError
import gdata.youtube.service
from werkzeug.security import check_password_hash, generate_password_hash
from lepl.apps.rfc3696 import Email

def make_app():
    return Flask("MovieFinder")

app = make_app()
app.config.from_object('settings')

if not app.debug:
    import logging
    from logging.handlers import TimedRotatingFileHandler
    file_handler = TimedRotatingFileHandler("logs/app.log")
    file_handler.setLevel(logging.WARNING)
    app.logger.addHandler(file_handler)

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
    rating_uk       = db.Column(db.String(), nullable=True)
    rating_usa      = db.Column(db.String(), nullable=True)
    languages       = db.Column(db.String(), nullable=True)
    plot_outline    = db.Column(db.String(), nullable=True)
    stars           = db.Column(db.String(), nullable=True)

    trailer_url = db.Column(db.String(), nullable=True)
    trailer_cached = db.Column(db.DateTime(), nullable=True)

    # Scores + recommendations + date cached
    date_cached     = db.Column(db.DateTime(), nullable=True)
    tomatoes_score  = db.Column(db.Integer(), nullable=True)
    imdb_score      = db.Column(db.Float(), nullable=True)
    recomendations = db.Column(postgres.ARRAY(db.Integer), nullable=True)

    def get_poster_url(self):
        return url_for("static", filename="posters/%s/%s.jpg"%(str(self.imdb_id)[0], self.imdb_id))

    def toJson(self, linked_by=None):
        x =  {jk:getattr(self, k) or "unknown" for jk,k in [
            ("id","imdb_id"), ("poster","poster_url"),
            ("title","title"),("year","year"),("director","director"),
            ("score","imdb_score"),("imdb_id","imdb_string_id"),
            ("genre","genre"),("rating_uk","rating_uk"),("rating_usa","rating_usa"),
            ("languages","languages"),("plot_outline","plot_outline"),
            ("stars","stars"),("tomatoes_score", "tomatoes_score")
        ]}
        if linked_by:
            x["linked_by"] = ", ".join(linked_by)
        else:
            x["linked_by"] = ""
        return x


class User(db.Model):
    __tablename__ = "users"
    user_id = db.Column(db.Integer(), primary_key=True)
    # For signed up accounts
    user_email = db.Column(db.String(), nullable=True, unique=True)
    user_password = db.Column(db.String(), nullable=True)
    # For facebook accounts
    fb_user_id = db.Column(db.BigInteger(), nullable=True, unique=True)

    movies_liked = db.Column(postgres.ARRAY(db.Integer), default=[])
    movies_hidden = db.Column(postgres.ARRAY(db.Integer), default=[])
    movies_queued = db.Column(postgres.ARRAY(db.Integer), default=[])


    def get_movies_liked(self):
        if not len(self.movies_liked):
            return []

        return Movie.query.filter(Movie.imdb_id.in_(self.movies_liked)).all()

try:
    db.session.execute("CREATE extension intarray")
except ProgrammingError:
    db.session.rollback()


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

@celery.task(name="MovieFinder.GetRottenTomatoesScore", rate_limit="1/s", default_retry_delay=100)
def GetRottenTomatoesScore(imdb_id):
    with celery.flask_app.test_request_context():
        return _GetRottenTomatoesScore(imdb_id)

def _GetRottenTomatoesScore(imdb_id):
    try:
        movie_db = db.session.query(Movie).filter_by(imdb_id=imdb_id).one()
    except Exception:
        print "[RottenTomatoes] Movie %s not found"%imdb_id
        return

    api_url = "http://api.rottentomatoes.com/api/public/v1.0/movie_alias.json?type=imdb&id=%s&apikey=%s"%(
        movie_db.imdb_string_id, app.config["ROTTEN_TOMATOES_API_KEY"])
    try:
        data = json.loads(urllib2.urlopen(api_url).read())
    except urllib2.HTTPError,e:
        if e.code == 403:
            print "[RottenTomatoes] API error 403, retrying in 100"
            return GetRottenTomatoesScore.retry()
        print "[RottenTomatoes] Could not get rotten tomatoes score: %s"%e
        print api_url
        return

    if "error" in data:
        print "[RottenTomatoes] Could not get tomatoes score for %s (%s): %s"%(movie_db.title, movie_db.imdb_string_id,
                                                                               data["error"])
        return

    try:
        movie_db.tomatoes_score = data["ratings"]["critics_score"]
    except KeyError:
        print "[RottenTomatoes] Error getting ratings/critics_rating key"
        return

    if movie_db.tomatoes_score >= 0:
        db.session.add(movie_db)
        try:
            db.session.commit()
            print "[RottenTomatoes] Movie '%s' scored %s"%(movie_db.title, movie_db.tomatoes_score)
        except Exception,e:
            print "[RottenTomatoes] Error committing score: %s"%e
            db.session.rollback()
    else:
        print "[RottenTomatoes] Movie '%s' scored below 0"%(movie_db.title)

@celery.task(name="MovieFinder.GetRecommendations")
def GetRecommendations(imdb_id):
    with celery.flask_app.test_request_context():
        return _GetRecommendations(imdb_id)

def _GetRecommendations(movie_id):
    try:
        movie_db = db.session.query(Movie).filter_by(imdb_id=movie_id).one()
    except Exception:
        print "Movie %s not found"%movie_id
        return

    print "Fetching recommendations for id %s"%movie_id
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


@celery.task(name="MovieFinder.AddMovie", default_retry_delay=30)
def AddMovie(*args, **kwargs):
    with celery.flask_app.test_request_context():
        return _AddMovie(*args, **kwargs)

def _AddMovie(movie_id, get_recommendations=True):
    print "Processing movie %s"%movie_id

    string_id = movie_id
    movie_id = int(movie_id) # Truncates the leading 0's :(

    try:
        movie_db = db.session.query(Movie).filter_by(imdb_id=movie_id).one()
    except Exception:
        movie_db = Movie()
        movie_db.imdb_id = movie_id
        movie_db.imdb_string_id = string_id

    imdb = IMDb()
    try:
        movie = imdb.get_movie(movie_id)
    except IMDbDataAccessError:
        print "Error accessing IMDB for ID %s, retrying"%movie_id
        AddMovie.retry()
        return
    if not movie.get("title"):
        print "Movie %s not in IMDB apparently, retrying"%movie_id
        AddMovie.retry()
        return

    if "production status" in movie.keys():
        print "Movie ID %s is not completed yet (%s) - stopping"%(movie_id,movie.get("production status"))
        return

    movie_db.title = movie.get("title")
    movie_db.year = movie.get("year")

    if movie.get("director"):
        # use the 1st one
        movie_db.director = movie.get("director")[0].get("name")

    movie_db.imdb_score = movie.get("rating")
    movie_db.date_cached = datetime.datetime.now()

    if "genres" in movie.keys():
        movie_db.genre = ", ".join(movie.get("genres"))

    if "certificates" in movie.keys():
        certs = movie.get("certificates")
        for cert in certs:
            csplit = cert.split(":")
            if csplit[0] == "UK":
                movie_db.rating_uk = csplit[1]
            elif csplit[0] == "USA":
                movie_db.rating_usa = csplit[1]

    if "languages" in movie.keys():
        movie_db.languages = ", ".join(movie.get("languages"))

    if "plot outline" in movie.keys():
        movie_db.plot_outline = movie.get("plot outline")

    if "cast" in movie.keys():
        movie_db.stars = ", ".join(
            [o.get("name") for o in movie.get("cast")[:3] if o.get("name")]
        )

    db.session.add(movie_db)
    try:
        db.session.commit()
    except Exception,e:
        print "Error adding ID %s: %s"%(movie_id,e)
        db.session.rollback()
        return
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
                #print "Downloading poster to path %s"%_path
                with open(_path,"wb") as poster:
                    poster_req = urllib2.Request(movie.get("cover url"))
                    wr = urllib2.urlopen(poster_req, timeout=5)
                    while True:
                        d = wr.read(100)
                        if d == "":
                            break
                        poster.write(d)
                print "Poster downloaded"
                movie_db.poster_url = _path
            else:
                print "Poster already downloaded"
                movie_db.poster_url = _path
            db.session.add(movie_db)
            try:
                db.session.commit()
            except Exception,e:
                print "Could not set poster_url: %s"%e
                db.session.rollback()
        except Exception,e:
            print "Error downloading poster: %s"%e
    else:
        print "* Setting cover URL to nothing"
        movie_db.poster_url = os.path.join("static","img","no_cover_art.gif")
        db.session.add(movie_db)
        try:
            db.session.commit()
        except Exception,e:
            print "Could not set poster_url: %s"%e
            db.session.rollback()

    if get_recommendations:
        print "Getting recommendations..."
        GetRecommendations.apply_async((movie_db.imdb_id,))

    if not movie_db.tomatoes_score:
        GetRottenTomatoesScore.apply_async((movie_db.imdb_id,))

    db.session.close()

def render_template(*args, **kwargs):
    kwargs["user"] = get_user()
    return _old_render(*args, **kwargs)

@app.route("/")
def index():
    user = get_user()

    if not user:
        return render_template("connect.html")

    return render_template("index.html", placeholder=random_movie(), user=user)



#@app.route("/api/get_trailer/<int:id>")
def get_trailer(id):
    try:
        movie = db.session.query(Movie).filter_by(imdb_id=id).one()
    except Exception:
        return abort(404)

    if movie.trailer_cached:
        if (datetime.datetime.now()-movie.trailer_cached) > datetime.timedelta(days=3):
            return json.dumps({"url":movie.trailer_cached})

    youtube =  gdata.youtube.service.YouTubeService()
    query =  gdata.youtube.service.YouTubeVideoQuery()
    query.vq = "%s (%s) trailer"%(movie.title, movie.year or "")

    feed = youtube.YouTubeQuery(query)
    if len(feed.entry):
        vidya = feed.entry[0].link[0].href.replace("watch?v=",'v/')
        movie.trailer_cached = datetime.datetime.now()
        movie.trailer_url = vidya
        try:
            db.session.add(movie)
            db.session.commit()
        except Exception,e:
            print "Cannot add movie cache: %s"%e
            db.session.rollback()
        return vidya
        #return trailer_source % (vidya, vidya)
    return "not_found"


@app.route("/api/randommovie")
def random_movie():
    try:
        return db.session.query(Movie.title).order_by(func.random()).limit(1).one()[0]
    except Exception:
        return ""

@app.route("/api/queue/<int:id>", methods=["PUT","GET","DELETE"])
def addtoqueue(id):
    user = get_user()
    if not user:
        return abort(400)
    if request.method == "GET":
        try:
            db_item = db.session.query(Movie).filter_by(imdb_id=id).one()
            return json.dumps(db_item.toJson())
        except Exception:
            return abort(404)

    if request.method == "PUT":
        try:
            data = json.loads(request.data)
        except Exception:
            return abort(400)

        if not id in user.movies_queued:
            db.session.query(User).filter(User.user_id == user.user_id).update(
                    {User.movies_queued:User.movies_queued.op("+")([int(data["id"])])}, synchronize_session=False
            )
            db.session.commit()

        return "saved"


@app.route("/api/getqueue",methods=["GET"])
def getqueue():
    user = get_user()
    if not user:
        return abort(400)
    return json.dumps([x.toJson() for x in db.session.query(Movie) \
                                                                    .filter(Movie.imdb_id.in_(user.movies_queued)).all()
                    ])

@app.route("/api/getrecommendations")
def recommendations():

    import time
    t1 = time.time()

    user = get_user()

    if not user:
        return abort(400)

    user_likes = user.movies_liked
    user_queue = user.movies_queued
    shit_they_like = db.session.query(Movie.imdb_id, Movie.title, Movie.recomendations)\
                                                .filter(and_(Movie.imdb_id.in_(user_likes),
                                                            not_(Movie.imdb_id.in_(user_queue)))) \
                                                .filter(Movie.recomendations != None) \
                                                .filter(Movie.recomendations != []).limit(150).all()


    id_counters = defaultdict(int)
    for item in shit_they_like:
        for id in item[2]:
            if not ((id in user.movies_hidden) or (id in user.movies_queued)) and not id in user.movies_liked:
                id_counters[id]+=1

    sorted_stuff = sorted(id_counters.iteritems(), key=operator.itemgetter(1))
    item_ids = sorted_stuff[-10:]
    item_ids.reverse()
    items = {x.imdb_id:x for x in db.session.query(Movie).filter(Movie.imdb_id.in_([x[0] for x in item_ids])).all()}

    linked_by = {}
    for movie in items:
        x = []
        for id, title, movie_recommendations in shit_they_like:
            if items[movie].imdb_id in movie_recommendations:
                x.append(title)
        linked_by[items[movie].imdb_id] = x

    time_taken = time.time() - t1
    print "Time taken to process recommendations: %s"%time_taken
    return json.dumps([items[i].toJson(linked_by=linked_by[items[i].imdb_id]) for i in items])


@app.route("/api/recommendation", methods=["PUT"])
def recommendation():
    user = get_user()
    if not user:
        return abort(400)

    try:
        data = json.loads(request.data)
    except Exception:
        return abort(400)
    if "queued" in data:
        if data["queued"]:
            print "Queuing %s"%data["id"]
            db.session.query(User).filter(User.user_id == user.user_id).update(
                    {User.movies_queued:User.movies_queued.op("+")([int(data["id"])])}, synchronize_session=False
            )
        else:
            print "Unqueueing %s"%data["id"]
            db.session.query(User).filter(User.user_id == user.user_id).update(
                    {User.movies_queued:User.movies_queued.op("-")([int(data["id"])])}, synchronize_session=False
            )

    if "hidden" in data:
        print "hidden shit"
        if not int(data["id"]) in user.movies_hidden:
            print "hiding..."
            db.session.query(User).filter(User.user_id == user.user_id).update(
                    {User.movies_hidden:User.movies_hidden.op("+")([int(data["id"])])}, synchronize_session=False
            )
    db.session.commit()

    return request.data


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

    if id in user.movies_queued:
        db.session.query(User).filter(User.user_id == user.user_id).update(
                {User.movies_queued:User.movies_queued.op("-")([id])}, synchronize_session=False
        )
        db.session.commit()

    if not id in user.movies_liked:
        db.session.query(User).filter(User.user_id == user.user_id).update(
                {User.movies_liked:User.movies_liked.op("+")([id])}, synchronize_session=False
        )

        db.session.commit()

    return request.data


class InvalidPassword(Exception):
    pass


@app.route("/signup_account", methods=["POST","GET"])
def signup_account():
    if request.method == "GET":
        return redirect("/")
    email = request.form.get("email", None)
    password = request.form.get("password", None)
    password_check = request.form.get("password2", None)

    if not all([email, password, password_check]):
        return render_template("connect.html", signup_error='NotGiven',
        email_signup=email)

    if not password == password_check:
        return render_template("connect.html", signup_error="PasswordMatch",
        email_signup=email)

    if db.session.query(User).filter(User.user_email == email).count():
        return render_template("connect.html", signup_error="EmailExists",
                    email_signup=email)
    else:
        vd = Email()
        if not vd(email):
            return render_template("connect.html", signup_error="InvalidEmail",
                    email_signup=email)
        # Make an account
        user = User()
        user.user_email = email
        user.user_password = generate_password_hash(password)
        db.session.add(user)
        db.session.commit()
        session["auth_user"] = user.user_id

        return redirect("/")


@app.route("/login_account", methods=["POST", "GET"])
def login_account():
    if request.method == "GET":
        return redirect("/")
    email = request.form.get("email", None)
    password = request.form.get("pass", None)

    if not all([email, password]):
        return render_template("connect.html", login_error='NotGiven',
        email=email)

    try:
        user_db = db.session.query(User).filter(User.user_email == email).one()
        if not check_password_hash(user_db.user_password, password):
            raise InvalidPassword
    except (NoResultFound, InvalidPassword):
        return render_template("connect.html", login_error="InvalidCredentials",
            email=email)

    # Validated!
    session["auth_user"] = user_db.user_id
    return redirect("/")



@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("index"))

@app.route('/login')
def login():
    return facebook.authorize(callback=url_for('facebook_authorized',
        next=request.args.get('next') or request.referrer or None,
        _external=True))


@app.route('/login/authorized')
@facebook.authorized_handler
def facebook_authorized(resp):
    if resp is None:
        return 'Access denied: reason=%s error=%s' % (
            request.args['error_reason'],
            request.args['error_description']
            )
    session['oauth_token'] = (resp['access_token'], '')
    me = facebook.get('/me')

    try:
        user = db.session.query(User).filter_by(fb_user_id=me.data["id"]).one()
    except NoResultFound:
        user = User()
        user.fb_user_id = me.data["id"]
        db.session.add(user)
        db.session.commit()
        print "Created a user account for ID %s"%me.data["id"]

    session["auth_user"] = user.user_id

    return redirect("/#search")


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

    app.run(host="0.0.0.0")