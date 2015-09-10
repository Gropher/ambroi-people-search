function getUrlParameter(sParam)
{
    var sPageURL = window.location.search.substring(1);
    var sURLVariables = sPageURL.split('&');
    for (var i = 0; i < sURLVariables.length; i++) 
    {
        var sParameterName = sURLVariables[i].split('=');
        if (sParameterName[0] == sParam) 
        {
            return sParameterName[1];
        }
    }
}        

function statusChangeCallback(response) {
  console.log('statusChangeCallback');
  console.log(response);
  if (response.status === 'connected') {
    Cookies.set('facebook_access_token', response.authResponse.accessToken, { expires: 1 });
    $('.login').hide();
    $('.content').show();
    if(getUrlParameter('q')){
      $('#search-query').val(decodeURIComponent(getUrlParameter('q')));
    }    
    search();
  } else {
    Cookies.remove('facebook_access_token');
    $('.login').show();
    $('.content').hide();
  }
}

function checkLoginState() {
  FB.getLoginStatus(function(response) {
    statusChangeCallback(response);
  });
}

function searchSubmit() {
  var query = $('#search-query').val();
  if(query == ''){ return false; }
  window.location.href = 'http://search.ambroi.net/?q='+query;
}

function search() {
  var query = $('#search-query').val();
  if(query == ''){ return false; }
  var spinner = new Spinner().spin();
  $('.spinner').html('');
  $('.spinner').append(spinner.el);
  $('.search-button').hide();
  $('.searching-button').show();
  $('.results').html('');
  $('.searching-button').html('Searching...');
  $.post('/search', {q: query}, function(data){
    if(JSON.parse(data)['data'] == 'success'){
      interval_id = setInterval(function(){
        $.get('/search_results?q='+query, function(data){
          results = JSON.parse(data)['data'];
          if(results['status'] == 'finished') {
            $('.search-button').show();
            $('.searching-button').hide();
            spinner.stop();
            clearInterval(interval_id);
          }
          if(results['progress'] > 0) {
            $('.searching-button').html('Searching... ('+results['progress']+'%)');
          }
          $('.results').html('');
          $.each(results['results'], function(i, value) {
            links = '';
            $.each(value['links'], function(j, link) {
              links = links + '<li><a target="_blank" href="'+link+'">'+link+'</a></li>'
            });
            $('.results').append('<li class="list-group-item"><strong><a target="_blank" class="black-link" href="https://www.bing.com/search?q='+value['name']+'">'+value['name']+'</a></strong><span class="badge">'+value['links'].length+'</span><ul>'+links+'</ul></li>');
          }); 
        });
      },5000);
    }
  });
}

window.fbAsyncInit = function() {
  FB.init({
    appId      : '444820295577045',
    cookie     : true,
    xfbml      : true,
    version    : 'v2.2'
  });
  FB.getLoginStatus(function(response) {
    statusChangeCallback(response);
  });
};

(function(d, s, id) {
  var js, fjs = d.getElementsByTagName(s)[0];
  if (d.getElementById(id)) return;
  js = d.createElement(s); js.id = id;
  js.src = "//connect.facebook.net/en_US/sdk.js";
  fjs.parentNode.insertBefore(js, fjs);
}(document, 'script', 'facebook-jssdk'));
