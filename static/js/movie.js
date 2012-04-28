
window.Movie = Backbone.Model.extend({
    urlroot:"/api/movies"
});

window.MovieCollection = Backbone.Collection.extend({
    model:Movie,
    url:"/api/movies"
});

window.MovieRecomendation = Backbone.Model.extend({
    url:"/api/recommendation"
});

window.MovieRecommendationCollection = Backbone.Collection.extend({
    model:MovieRecomendation,
    url:"/api/getrecommendations"
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
        console.log(this.model.models);
        var template = _.template($("#recommendations_view").html(), {objects:this.model.models});
        $("#main").html( template );

        return this;
    }
});

window.RecommendationItemView = Backbone.View.extend({
    template:_.template($("#template_recommendation_item").html()),

    render: function(eventName) {
        $(this.el).html(this.template({object:this.model.toJSON()}));
    }
});

function handleSearchClick(id, title){

    var movie = new Movie({id:id, title:title});
    app.movielist.add(movie);
    movie.save();
    $("#"+id+"_search").hide();
}

function handleRecommendedLikeClick(id){
    // Make a new movie and add it to the shiz
    var recommended = app.movieSuggestions.get(id);

    var movie = new Movie({"id":recommended.get("imdb_id"),
                       "title":recommended.get("title")});
    app.movielist.add(movie);
    movie.save();

    app.movieSuggestions.remove(recommended);
    app.movieSuggestions.fetch();
}

function handleRecommendedDislikeClick(id){
    // Make sure the user doesn't see this film again
    var recommended = app.movieSuggestions.get(id);

    recommended.set("hidden",true);
    recommended.save();
    app.movieSuggestions.remove(recommended);
    app.movieSuggestions.fetch();

}

var AppRouter  = Backbone.Router.extend({
    routes:{
        "":"recommendations",
        "search":"search"
    },

    search:function(){
        console.log("rendering bar...");
        $("#main").html($("#search_view").html());
        console.log("main html done");
        $("#pheader").text("Search");
        console.log("search set");
        $("#button_show_rec").show();
        $("#button_show_search").hide();

        window.setupSearch();

        this.renderSideBar()
    },

    renderSideBar:function(){
        this.movielist = new MovieCollection();
        this.movielistView = new MovieView({model:this.movielist});
        this.movielist.fetch();
    },


    recommendations:function() {
        this.movieSuggestions = new MovieRecommendationCollection();
        var recommend_view = new RecommendationsView({model:this.movieSuggestions});
        this.movieSuggestions.fetch();
        $("#button_show_rec").hide();
        $("#button_show_search").show();
        this.renderSideBar();
    }

});

var app = new AppRouter();
Backbone.history.start();