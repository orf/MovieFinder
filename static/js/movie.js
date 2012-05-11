
window.Movie = Backbone.Model.extend({
    urlroot:"/api/movies"
});

window.MovieCollection = Backbone.Collection.extend({
    model:Movie,
    url:"/api/movies"
});

window.MovieRecomendation = Backbone.Model.extend({
    url:"/api/recommendation",
    urlroot:"/api/recommendation"
});

window.MovieQueueItem = Backbone.Model.extend({
    url:function(){
        return "/api/queue/"+this.get("id");
    }
});

window.MovieRecommendationCollection = Backbone.Collection.extend({
    model:MovieRecomendation,
    url:function(){
        var filter_type = $("#filter_type").val();
        var filter_score = $("#filter_imdb_score").val();
        var url = "/api/getrecommendations?type=" + filter_type + "&imdb_score=" + filter_score;
        if ($("#language_input").val() != ""){
            url = url + "&language="+$("#language_input").val();
        }
        if ($("#genre_input").val() != ""){
            url = url + "&genre="+$("#genre_input").val();
        }
        if ($("#director_input").val() != ""){
            url = url + "&director="+$("#director_input").val();
        }

        return url
    }
    //urlroot:"/api/recommendation"
});

window.MovieQueueCollection = Backbone.Collection.extend({
    model:MovieRecomendation,
    url:"/api/getqueue"
});


window.MovieView = Backbone.View.extend({
    el: $("#movieList"),

    initialize:function () {
        var that = this;
        this.model.bind("reset", this.render, this);
        this.model.bind("remove", function(movie){
            this.render();
        }, this);
        this.model.bind("add", function(movie) {
            $("#movieList").append(new MovieItemView({model:movie}).render().el);
            $(".delete_movie_item").click(function(ev){
                ev.preventDefault();
                that.handleDeleteClick(this);
            })
        });
    },

    handleDeleteClick:function(obj){
        var id = $(obj).attr("data-id");
        var item = this.model.get(id);
        if (item){
            item.destroy();
        }
        app.movieSuggestions.fetch();
    },

    render:function (eventName) {
        console.log("shit called");
        var that = this;
        $("#movieList").html("");
        _.each(this.model.models, function (movie) {
            $("#movieList").append(new MovieItemView({model:movie}).render().el);
        }, this);

        $(".delete_movie_item").click(function(ev){
            ev.preventDefault();
            that.handleDeleteClick(this);
        });

        return this;
    }
});

window.MovieItemView = Backbone.View.extend({
    template:_.template($("#template_movie_item").html()),

    render: function (eventName) {
        $(this.el).html(this.template({object:this.model.toJSON()}));
        return this;
    }
});

window.RecommendationsView = Backbone.View.extend({
    initialize: function(){
        var that = this;
        this.model.bind("reset", this.render, this);
        this.model.bind("add", function(movie) {
            that.render();
        });
        this.model.bind("remove", function(){
            that.render();
        });
        this.render();
    },

    render: function(){
        $("#pheader").text("Your recommendations");
        var template = _.template($("#recommendations_view").html(), {objects:this.model.models});
        $("#main").html( template );

        $(".remove_button").click(function(){
            var movie = app.moviequeue.get($(this).attr("x-id"));
            app.moviequeue.remove(movie);
            movie.set("queued", false);
            movie.save();
            $(this).removeClass("btn btn-inverse remove_button").addClass("btn btn-primary")
                    .attr("title", "Add to my watchlist");
            console.log(this);
            $(this).unbind("click");
            $(this).click(function(){
                HandleAddQueueClick(movie.get("id"));
            });
            $(this).find("i").removeClass("icon-minus icon-white").addClass("icon-plus icon-white");
        });

        return this;
    }
});

window.RecommendationItemView = Backbone.View.extend({
    template:_.template($("#template_recommendation_item").html()),

    render: function(eventName) {
        $(this.el).html(this.template({object:this.model.toJSON()}));
    }
});

window.MovieQueueView = Backbone.View.extend({
    initialize: function(){
        var that = this;
        this.model.bind("reset", this.render, this);
        this.model.bind("add", function(movie) {
            that.render();
        });
        this.model.bind("remove", function(){
            that.render();
        });
        this.render();
    },

    render: function(force){
        console.log(this.model.models.length);
        if (this.model.models == 0){
            $("#movieQueue").html("");
            return
        }
        var template = _.template($("#moviequeue_template").html(), {objects:this.model.models});
        $("#movieQueue").html( template );

        $(".view_queue_item").click(function(){
            var movie = app.moviequeue.get($(this).attr("data-id"));
            if (movie){
                app.movieSuggestions.reset();
                app.movieSuggestions.add(movie);
            }
        })

    }
});

function handleSearchClick(id, title){
    var movie = new Movie({id:id, title:title});
    app.movielist.add(movie);
    movie.save();
    $("#"+id+"_search").hide();
}

function handleSearchQueueClick(id, title){
    var _id = id.replaceAll("tt","");
    var movie = new MovieQueueItem({id:_id, title:title, from_search:true});
    movie.save();
    app.moviequeue.add(movie);
}

function handleSearchRemoveQueueClick(id){
    console.log(id);
}

function HandleAddQueueClick(id){
    var movie = app.movieSuggestions.get(id);
    console.log("Movie:");
    console.log(movie);
    app.moviequeue.add(movie);
    app.movieSuggestions.remove(movie);
    movie.set("queued",true);
    movie.save({},{success:function(){
        app.movieSuggestions.fetch();
    }, error: function(){
        app.movieSuggestions.fetch()
    }});
}


function handleRecommendedLikeClick(id){
    // Make a new movie and add it to the shiz
    var recommended = app.movieSuggestions.get(id);

    var movie = new Movie({"id":recommended.get("imdb_id"),
                       "title":recommended.get("title")});
    app.movielist.add(movie);
    movie.set("queue", false);
    movie.save({}, {success:function(){
        app.moviequeue.remove(recommended);
        app.movieSuggestions.remove(recommended);
        app.movieSuggestions.fetch();
    }, error: function(){
        app.moviequeue.remove(recommended);
        app.movieSuggestions.remove(recommended);
        app.movieSuggestions.fetch();
    }});
}

function handleRecommendedDislikeClick(id){
    // Make sure the user doesn't see this film again
    var recommended = app.movieSuggestions.get(id);
    recommended.save({"hidden":true, "queue":false}, {success:function(){
        app.moviequeue.remove(recommended);
        app.movieSuggestions.remove(recommended);
        app.movieSuggestions.fetch();
    }, error: function(){
        app.moviequeue.remove(recommended);
        app.movieSuggestions.remove(recommended);
        app.movieSuggestions.fetch();
    }});
}

var AppRouter  = Backbone.Router.extend({
    routes:{
        "search":"search",
        "":"recommendations",
        "queue/:id":"view_queue_id"
    },

    view_queue_id:function(id){
        console.log(id);
    },

    search:function(){
        console.log("rendering bar...");
        $("#main").html($("#search_view").html());
        console.log("main html done");
        $("#pheader").text("Search");
        console.log("search set");
        $("#button_show_rec").show();
        $("#button_show_search").hide();
        $("#filter_results").hide();
        $("#side_refresh").hide();

        window.setupSearch();

        this.renderSideBar()
    },

    renderSideBar:function(){
        this.movielist = new MovieCollection();
        this.movielistView = new MovieView({model:this.movielist});
        this.movielist.fetch();

        this.moviequeue = new MovieQueueCollection();
        this.moviequeueView = new MovieQueueView({model:this.moviequeue});
        this.moviequeue.fetch();

    },


    recommendations:function() {
        this.movieSuggestions = new MovieRecommendationCollection();
        var recommend_view = new RecommendationsView({model:this.movieSuggestions});
        this.movieSuggestions.fetch();
        $("#button_show_rec").hide();
        $("#button_show_search").show();
        $("#filter_results").show();
        $("#side_refresh").show();
        this.renderSideBar();
    }

});

var app = new AppRouter();
Backbone.history.start();