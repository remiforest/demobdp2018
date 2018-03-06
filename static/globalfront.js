var selector_cities = []
var cities = [];
var count=true;
var title_desc = "count"
var consolidate = true;
var updating_charts = true;

function contains(array,string){
    var in_array = false; 
    for (var i=0;i<array.length;i++){
        if (array[i]==string){
            in_array = true;
            break;
        }
    }
    return in_array;
}



function create_charts(){
    console.log("create charts for " + cities + "(consolidate = "+consolidate+")");
    if (consolidate){
        if (!$("#Global").length){create_chart("Global");}
    }else{
        for (var i=0;i<cities.length;i++){
            var city = cities[i];
            if (!$("#"+city).length){create_chart(city);}
        }
    }
}

function create_chart(city){
    if (!$('#'+city).length){
        $('#charts').append("<div class='col-md-6' id="+city+"></div>");
    }
    var myChart = Highcharts.chart(city, {
        chart: {
            type: 'column'
        },
        title: {
            text: city + " " + title_desc
        },
        xAxis: {
            categories: []
        },
        yAxis: {
            title: {
                text: 'Count'
            }
        },
        series: [{
            data: []
        }]
    });
    return myChart;
}




function update_charts(){
    updating_charts = false;
    console.log("update charts for " + cities + "(consolidate = "+consolidate+")");
    // Create missing charts if needed
    create_charts();
    $.ajax({
        url: 'get_stream_data',
        type: 'post',
        data:{"cities":JSON.stringify(cities),"consolidate":consolidate,"count":count},
        success:function(data){
            console.log(data);
            loaded_data = JSON.parse(data);
            console.log()
            // Iterate over object
            $.each(loaded_data,function(key,value){
                // get chart
                if (key == "Global" || contains(cities,key)){
                    var chart=$("#"+key).highcharts();
                    var new_data = [];
                    var categories = [];
                    // get and update values
                    if (chart){
                        var chart_data = chart.series[0].data;
                        //console.log(chart_data);
                        for (var i = 0; i < chart_data.length; i++) {
                            category = chart_data[i].category;
                            categories.push(category);
                            current_value = chart_data[i].y;
                            incoming_value = value[category];
                            if (incoming_value){
                                new_value = current_value + incoming_value;
                            }else{
                                new_value = current_value;
                            }
                            new_data.push(new_value);
                            delete value[category];
                        }
                        for (var category in value){
                            if ((category != "count" && (count || category == "gkm")) || category != "count" && category != "gkm"){
                                categories.push(category);
                                new_data.push(value[category]);                        
                            }
                        }
                        chart.xAxis[0].setCategories(categories);
                        chart.series[0].setData(new_data);
                    }
                }
            })
            
            updating_charts = true;
            setTimeout(function(){
                update_charts();
              }, 1000 * 2);
        }
    });
}




function update_stream_selector(){
    $.ajax({
        url: 'get_all_streams',
        type: 'get',
        success:function(data){
            console.log("available streams : " + data);
            var available_cities = JSON.parse(data);
            for (var i=0;i<available_cities.length;i++){
                var city = available_cities[i];
                if (! contains(selector_cities,city)){
                    if ($("#all_streams").is(':checked')){ checked = 'checked="checked"'}else{checked=""}
                    
                    $("#stream_selector").append($( "<span id='" + city + "_selector'><input class='city' type='checkbox' data-city='"+city+"' name='stream' value='city' "+ checked + " > " + city + "</span><p>"));
                    selector_cities.push(city);
                    if (checked){cities.push(city);}
                }
            }
            for (var i=0;i<cities.length;i++){
                var city = cities[i];
                if (! contains(available_cities,city)){
                    $("#" + city + "_selector").remove();
                    $("#" + city).remove();
                }
            }
            setTimeout(function(){
                update_stream_selector();
              }, 1000 * 5);
        }
    });
}



function update_events(){
    console.log("updating events");
    $.ajax({
        url: 'get_events_count',
        type: 'get',
        success:function(data){
            var current_count = $("#event_count").data("count");
            console.log("current count : " + current_count);
            new_count = JSON.parse(data).count;
            console.log("new count : " + new_count);
            $("#event_count").data("count",new_count);
            $("#event_count").prop('Counter',current_count).animate({
                    Counter: new_count
                }, {
                    duration: 5000,
                    easing: 'linear',
                    step: function (now) {
                        $(this).text(Math.ceil(now));
                    }
                });
            
            setTimeout(function(){
                update_events();
              }, 1000 * 5);
        }
    });

}


$("#deploy_new_country_btn").click(function(){
  var new_country = $("#new_country").val();
  console.log("deploying new country : " + new_country);
  $("#deploy_new_country_btn").text("Deploying ...");

  $.ajax({
      url: 'deploy_new_country',
      type: 'post',
      data: {"country":new_country},
      success:function(data){
          console.log(data);
          get_countries();
          $("#new_country").val("");
          $("#deploy_new_country_btn").text("Deploy");
      }
  });
})


function get_countries(){
    $.ajax({
        url: 'get_countries',
        type: 'get',
        success:function(data){
            console.log(data);
            var countries = JSON.parse(data)["countries"];
            console.log(countries);
            $("#countries").html("");
            for(var i=0;i<countries.length;i++){
                console.log(countries[i]._id);
                console.log(countries[i].port);
                $("#countries").append("<div class='row'><div class='col-md-9'><a href=http://global:" + countries[i].port + " target='_blank'>" + countries[i]._id + "</a></div>" +
                                       "<div class='col-md-3'><a href='#' data-country='" + countries[i]._id + "' class='remove_country text-danger float-right' > x</a></div></div>");
            }
        }
    });
}


$("#countries").on('click',".remove_country",function(){
  var country = $(this).data('country');
  console.log("removing country : " + country);
  
  $.ajax({
      url: 'remove_country',
      type: 'post',
      data: {"country":country},
      success:function(data){
          console.log(data);
          get_countries();
      }
  });
})
