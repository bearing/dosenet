var data_input = [];

//var url = 'https://radwatch.berkeley.edu/sites/default/files/dosenet/pinewood.csv?'
//+ Math.random().toString(36).replace(/[^a-z]+/g, ''); // To solve browser caching issue
function parseDate(input) {
  var parts = input.replace('-',' ').replace('-',' ').replace(':',' ').replace(':',' ').replace(',',' ').split(' ');
  // new Date(year, month [, day [, hours[, minutes[, seconds[, ms]]]]])
  return new Date(parts[0], parts[1]-1, parts[2], parts[3], parts[4], parts[5]); // Note: months are 0-based
}

function singleErrorPlotter(e) {
  var ctx = e.drawingContext;
  var points = e.points;
  var g = e.dygraph;
  var color = e.color;
  ctx.save();
  ctx.strokeStyle = e.color;

  for (var i = 0; i < points.length; i++) {
    var p = points[i];
    var center_x = p.canvasx;
    if (isNaN(p.y_bottom)) continue;

    var low_y = g.toDomYCoord(p.yval_minus),
        high_y = g.toDomYCoord(p.yval_plus);

    ctx.beginPath();
    ctx.moveTo(center_x, low_y);
    ctx.lineTo(center_x, high_y);
    ctx.stroke();
  }

  ctx.restore();
}

function process_csv(text,dose,time) {
  data_input = []; // Clear any old data out before filling!
  var lines = text.split("\n");
  var nentries = lines.length; // compare to full set possible for given time interval and keep smaller value
  var newest_data = lines[lines.length-2].split(",");
  var oldest_data = lines[1].split(",");
  var end_date = new Date(parseDate(newest_data[0]));
  var start_date = new Date(parseDate(oldest_data[0]));
  switch(time) {
	case 'Hour':
	  end_date = new Date(end_date.getTime() + -1*3600*1000);
	  nentries = Math.min(nentries,13); // 12 5 minute intervals in last hour
	break;
	case 'Day':
	  end_date = new Date(end_date.getTime() + -1*24*3600*1000);
	  nentries = Math.min(nentries,289); // 288 5 minute intervals in last day
	break;
	case 'Week':
	  end_date = new Date(end_date.getTime() + -7*24*3600*1000);
	  nentries = Math.min(nentries,2017); // 2016 in last week
	break;
	case 'Month':
	  end_date = new Date(end_date.getTime() + -30*24*3600*1000);
	  nentries = Math.min(nentries,8641); // 8640 in last month (30 days)
	break;
	case 'Year':
	  end_date = new Date(end_date.getTime() + -365*24*3600*1000);
	  nentries = Math.min(nentries,105121); // 105120 in last year
	break;
	case 'All':
	  end_date = new Date(end_date.getTime() - start_date.getTime());
	break;
  }

  var scale = 1.0;
  switch(dose) {
	case 'CPM':
	  scale = 1.0;
	break;
	case 'USV':
	  scale = 0.036;
	break;
	case 'REM':
	  scale = 0.0036;
	break;
	case 'cigarette':
	  scale = 0.036*0.00833333335;
	break;
	case 'medical':
	  scale = 0.036*0.2;
	break;
	case 'plane':
	  scale = 0.036*0.420168067;
	break;
  }

  for( var i = lines.length - nentries; i < lines.length; ++i ) {
    if( i < 1 ) { continue; } // skip first line(s) with meta-data
    var line = lines[i];
    if(line.length>3) {
      var data = line.split(",");
      var x = new Date(parseDate(data[0]));
      if( x.getTime() < end_date.getTime() ) { continue; }
      var y = parseFloat(data[1]);
      var err = parseFloat(data[2]);
      data_input.push([x,[y*scale,err*scale]]);
    }
  }
}

function plot_data(location,dose,time,div) {
  var title_text = location;
  var y_text = '&microSv/hr';
  var data_label = '&microSv/hr';
  var roll = 1;
  switch(dose) {
	case 'CPM':
	  title_text = title_text + ' in Counts per Minute ';
	  y_text = 'CPM';
	  data_label = "CPM";
	break;
	case 'USV':
	  title_text = title_text + ' in &microSv/hr ';
	  y_text = '&microSv/hr';
	  data_label = "&microSv/hr";
	break;
	case 'REM':
	  title_text = title_text + ' in &mrem/hr ';
	  y_text = 'mrem/hr';
	  data_label = "mrem/hr";
	break;
	case 'cigarette':
	  title_text = title_text + ' in cigarettes/hr ';
	  y_text = 'cigarettes/hr';
	  data_label = "cigarettes/hr";
	break;
	case 'medical':
	  title_text = title_text + ' in xRays/hr ';
	  y_text = 'xRays/hr';
	  data_label = "xRays/hr";
	break;
	case 'plane':
	  title_text = title_text + ' in airplane travel per hour ';
	  y_text = 'air-travel/hr';
	  data_label = "air-travel/hr";
	break;
  }
  switch(time) {
  	case 'Hour':
  	  title_text = title_text + 'for the most recent ' + time;
  	  roll = 1;
  	break;
  	case 'Day':
  	  title_text = title_text + 'for the most recent ' + time;
  	  roll = 1;
  	break;
  	case 'Week':
  	  title_text = title_text + 'for the most recent ' + time;
  	  roll = 7;
  	break;
  	case 'Month':
  	  title_text = title_text + 'for the most recent ' + time;
  	  roll = 30;
  	break;
  	case 'Year':
  	  title_text = title_text + 'for the most recent ' + time;
  	  roll = 365;
  	break;
  	case 'All':
  	  title_text = 'All data for ' + title_text;
  	  roll = 1;
  }

  g = new Dygraph(
    // containing div
    document.getElementById(div),
    data_input,
    { title: title_text,
	  rollPeriod: roll,
	  //showRoller: true,
      errorBars: true,
      connectSeparatedPoints: false,
      drawPoints: true,
      showRangeSelector: true,
      sigFigs: 3,
      ylabel: y_text,
      xlabel: 'Time (local)',
      labels: [ "Time (local)", data_label],
      data_label: {
                    strokeWidth: 0.0,
                    highlightCircleSize: 2,
                    plotter: [
                      singleErrorPlotter,
                      Dygraph.Plotters.linePlotter
                    ]
                  },
      axes: {
      	y: {
      		    //reserveSpaceLeft: 2,
          		axisLabelFormatter: function(x) {
        	  		                          			var shift = Math.pow(10, 5);
      		      		                          	return Math.round(x * shift) / shift;
        		      		                        }
      	   },
      }
    }
  );
}

function get_data(url,location,dose,time,div) {
  $.get(url, function (data) {
      var dataStr = new String(data);
      process_csv(dataStr,dose,time);
      plot_data(location,dose,time,div);
  },dataType='text');
}
