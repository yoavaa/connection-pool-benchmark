package com.wixpress.experiments

import java.io.PrintWriter
import collection.mutable

/**
 *
 * @author yoav
 * @since 5/8/13
 */
class WaterfallReport(val title: String) {

  private val stats: mutable.MutableList[Seq[Long]] = mutable.MutableList()

  private def minTime: Long = {
    stats
      .map(_.reduce(Math.min(_, _)))
      .reduce(Math.min(_, _))
  }
  private def maxTime: Long = {
    stats
      .map(_.reduce(Math.max(_, _)))
      .reduce(Math.max(_, _))
  }
  private def statsAsJson: String = {
    stats
      .sortBy(_.reduce(Math.min(_, _)))
      .map(_.mkString("[", ",", "]"))
      .mkString("[", ",\n", "]")
  }

  def addStats(stat: Seq[Long]) {
    synchronized {
      stats += stat
    }
  }

  def printToFile(fileName: String) {
    val out = new PrintWriter(fileName)
    try {
      print(out)
    }
    finally {
      out.flush()
      out.close()
    }
  }

  def reset {
    stats.clear()
  }

  def print(out: PrintWriter) {
    out.print(
      f"""<html>
  <head>
    <h1>$title%s</h1>
    <script language="javascript">
      var colors = ["#7d2f38", "#26af34", "#201a8a"]
      var minTime = $minTime%d;
      var maxTime = $maxTime%d;
      var stats = $statsAsJson%s
      window.onload = function() {
      	renderChart(minTime, maxTime);
      }
      function  renderChart(minTimeToRender, maxTimeToRender) {
        var target = document.getElementById("target");
        var totalTimeToRender = maxTimeToRender - minTimeToRender
        var graphHtml = "";
        var itemsToShow = 0;
        for (var i=0; i < stats.length; i++) {
          var stat = stats[i];
          var first = stat[0];
          var last =  stat[stat.length-1];
          if ((first < minTimeToRender && last > minTimeToRender) ||
          	  (first > minTimeToRender && last < maxTimeToRender) ||
          	  (first < maxTimeToRender && last > maxTimeToRender)) {
          	for (var step=0; step < stat.length-1; step++) {
            	var stepLeftPx = (stat[step] - minTimeToRender) / totalTimeToRender * 1500;
            	var stepWidthPx = (stat[step+1] - stat[step]) / totalTimeToRender * 1500;
            	var stepColor = colors[step % colors.length]
            	graphHtml += "<div style='position:absolute; background: "+stepColor+"; height: 1px; top:"+itemsToShow+"px; left:"+stepLeftPx+"px; width:"+stepWidthPx+"px'></div>"
          	}
          	itemsToShow += 1;
          }
        }
        target.style.height = itemsToShow;
        target.innerHTML = graphHtml;
        document.getElementById("min").value = (minTimeToRender - minTime) / 1000000000
        document.getElementById("max").value = (maxTimeToRender - minTime) / 1000000000
      }
      function refresh() {
      	var minInSeconds = document.getElementById("min").value
        var maxInSeconds = document.getElementById("max").value
        renderChart(minInSeconds * 1000000000 + minTime, maxInSeconds * 1000000000 + minTime)
      }
    </script>
  </head>
  <body>
    <div id="target" style="border: #000 1px solid;width:1500px;position:relative;"></div>
    <div id="zoom" style="position: absolute; top:0px; right: 0px">
    	<form>
    	  filter time (in seconds)</br>
    		min: <input id="min" type="number"></input>
    		max: <input id="max" type="number"></input>
  	  	<input id="ok" type="button" value="ok" onClick="refresh()"></input>
    	</form>
    </div>
  </body>
</html>
      """.stripMargin)
  }

}
