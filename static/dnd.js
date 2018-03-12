var active_streams = [];
var deployed_countries = [];

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

$('.drop').droppable({
  drop : function(e){
        var zone = $(this).attr('id');
        $.ajax({
            url: 'deploy_new_country',
            type: 'post',
            data: {"country":zone},
            success:function(data){
                $("#"+zone+"_docker_image").show();
            }
        });
    }
}); // ce bloc servira de zone de 


$('#recycle').droppable({
  drop : function(e,ui){
        var zone = ui.draggable.attr('id').split("_")[0];
        $("#"+zone+"_docker_image").hide();
        $.ajax({
              url: 'remove_country',
              type: 'post',
              data: {"country":zone},
              success:function(data){
                console.log(zone + " container stopped")
              }
          });
    }
}); // ce bloc servira de zone de dépôt


$(function(){

    $('.drag').draggable({revert:true,revertDuration: 0}); // appel du plugin

});



function display_streams(){
    $.ajax({
        url: 'get_active_streams',
        type: 'get',
        success:function(data){
            var current_streams = JSON.parse(data);
            console.log(current_streams);
            // Iterate over object
            $.each(current_streams,function(index,value){
                // get chart
                if (contains(active_streams,value)){
                  $("#" + value + "_streams").show();
                }
            });
            $.each(active_streams,function(index,value){
                // get chart
                if (!contains(current_streams,value)){
                  $("#" + value + "_streams").hide();
                }
            });
            active_streams = current_streams;
            if (active_streams.length>0){
              $("#replicate_streams").show();
            }else{
              $("#replicate_streams").hide();
              $("#global_streams").hide();
            }
            setTimeout(function(){
                display_streams();
              }, 1000);
        }
    });
}



$("#replicate_streams").click(function(){
    $("#replicate_streams").text("Replicating ...");
    $.ajax({
        url: 'replicate_streams',
        type: 'get',
        success:function(data){
            setTimeout(function(){
              $("#replicate_streams").text("Replicate streams");
              $("#global_streams").show();
            }, 1000 * 10);
          }
    });
});


function display_countries(){
    $.ajax({
        url: 'get_deployed_countries',
        type: 'get',
        success:function(data){
            var current_countries = JSON.parse(data);
            console.log(current_countries);
            // Iterate over object
            $.each(current_countries,function(index,value){
                // get chart
                if (contains(deployed_countries,value)){
                  $("#" + value + "_docker_image").show();
                }
            });
            $.each(active_streams,function(index,value){
                // get chart
                if (!contains(deployed_countries,value)){
                  $("#" + value + "_docker_image").hide();
                }
            });
            deployed_countries = current_countries;
            setTimeout(function(){
                display_countries();
              }, 1000);
        }
    });
}


$( document ).ready(function(){
  display_streams();
  display_countries();
});

