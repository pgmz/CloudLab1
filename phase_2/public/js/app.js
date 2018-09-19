$(document).ready(function() {

  //This array has arrays that include up to 4 image urls
  var imageArray = new Array();
  //indicates in which frame (that contains 4 images) we are. Index of previous array
  var currentFrames = 0;
  //how many images we are able to display in each frame
  const imagesPerFrame = 4;

  $("#version").html("v0.14");
  
  $("#searchbutton").click( function (e) {
    displayModal();
  });
  
  $("#searchfield").keydown( function (e) {
    if(e.keyCode == 13) {
      displayModal();
    }	
  });
  
  function displayModal() {
    $("#myModal").modal('show');

    $("#status").html("Searching...");
    $("#dialogtitle").html("Search for: "+$("#searchfield").val());
    $("#previous").hide();
    $("#next").hide();
    $.getJSON('/search/' + $("#searchfield").val() , function(data) {
      renderQueryResults(data);
    });
  }
  
  $("#next").click( function(e) {

    //show next frame
    currentFrames++;
    renderImages(imageArray[currentFrames]);

    //If user clicked "next", obviously is able to click "previous",
    //check if we are in last frame, if so, hide next button
    $("#previous").show();
    if(currentFrames == imageArray.length-1){
      $("#next").hide();
    }

  });
  
  $("#previous").click( function(e) {
    //show previous frame
    currentFrames--;
    renderImages(imageArray[currentFrames]);

    //If user clicked "previous", obviously is able to click "next",
    //check if we are in first frame, if so, hide previous button
    $("#next").show();
    if(currentFrames == 0){
      $("#previous").hide();
    }

  });

  function renderQueryResults(data) {

    if (data.error != undefined) {
      $("#status").html("Error: "+data.error);
    } else {
      $("#status").html(""+data.num_results+" result(s)");

      //reinitialize the array of images, to clear previous images...
      imageArray = new Array();

      //if there are more than 4 images, then we are able to show "next" images
      if(data.num_results > 4){
        $("#next").show();
      }      
      
      //used for indexing
      var frameCnt = 0;
      var ImageCnt = 0;
      currentFrames = 0;


      //the logic for images is: save in an array, arrays of 4 strings (image urls),
      //this way, if we press next or previous, we will index the big array, and get 4 images to render
      data.results.forEach(element => {

        //if the big array, doesn´t have an array at index frameCnt, initialize an array there
        if(imageArray[frameCnt] == null){
          imageArray[frameCnt] = new Array();
        }

        //store images in an inner array, if that inner array already has 4 images, create another
        //inner array
        imageArray[frameCnt][ImageCnt] = element;
        ImageCnt++
        if(ImageCnt == imagesPerFrame){
          ImageCnt = 0;
          frameCnt++;
        }
      });

      //render the array of images, at the index "currentFrames" in the big array
      renderImages(imageArray[currentFrames]);

     }
   }

   //This function will render all the images in an array called frames. limited to 4
   function renderImages(frames){
     //If somehow, the array "frames" has more than 4 elements, return...
     if(frames.length != imagesPerFrame){
      return;
     }

     //clear the contents of photox cells..
    $("#photo0").html("");$("#photo1").html("");$("#photo2").html("");$("#photo3").html("");

    //render images
    var cnt = 0;
    frames.forEach(function(image){
      $("#photo" + cnt).html("<img src=" + image
        + " alt=" + image + " height=\"250\" width=\"250\"/>");
      cnt++;
    });
   }

});
