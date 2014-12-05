var margin = {top: 20, right: 150, bottom: 30, left: 150};
var barWidth = 450;
var barHeight = 20;

var x = d3.scale.linear().range([0, barWidth]);
var color = d3.scale.quantize().range(colorbrewer.RdYlGn[11]);

var chart = d3.select(".chart")
    .attr("width", barWidth + margin.left + margin.right)
    .style("border-style", "none dashed none dashed")
    .style("border-color", d3.rgb("gray").brighter());

var allgroup = chart.append("g");

var cputip = chart.append("text")
    .style("text-anchor", "start")
    .style("visibility", "hidden");

var storagetip = chart.append("text")
    .style("text-anchor", "start")
    .style("visibility", "hidden");

chart.append("rect")
    .attr("x", margin.left)
    .attr("y", 5)
    .attr("width", 10)
    .attr("height", 10)
    .style("fill", d3.rgb("black").brighter());

chart.append("text")
    .attr("x", margin.left + 14)
    .attr("dy", 14)
    .style("fill", "gray")
    .style("text-anchor", "start")
    .text("Subscribed today");

var headerSvg = chart.append("svg")
    .attr("width", 330)
    .attr("x", margin.left + 120)
    .attr("y", 5);

var headerBoxes = headerSvg.append("g");

var colors = headerBoxes.selectAll("g")
    .data(colorbrewer.RdYlGn[11])
    .enter().append("g")
    .attr("transform", function(d, i) { return "translate(" + i*11 + ", 0)"; });

colors.append("rect")
    .attr("height", 10)
    .attr("width", 10)
    .style("fill", function(d) { console.log(d); return d});

headerSvg.append("text")
    .attr("x", 125)
    .attr("dy", 9)
    .style("fill", "gray")
    .style("text-anchor", "start")
    .text("Ratio CPU per GB");

d3.csv("system.csv", type, function(error, data) {

    x.domain([0, 1]);
    color.domain([d3.max(data, function(d) { return d.cpu/d.used; }), d3.min(data, function(d) { return d.cpu/d.used; })]);

    chart.attr("height", margin.top + barHeight * data.length);

    var site = allgroup.selectAll("g")
        .data(data)
        .enter().append("g")
        .attr("transform", function(d, i) { return "translate(0, " + (margin.top + (i * barHeight)) + ")"; })
        .on("mouseover", function(d, i){
            d3.select("#rec" + i).style("fill", function() { return d3.rgb(d3.select("#rec" + i).style("fill")).darker()});
            d3.select("#bar" + i).style("fill", function() { return d3.rgb(d3.select("#bar" + i).style("fill")).darker()});
            d3.select("#sub" + i).style("fill", function() { return d3.rgb(d3.select("#sub" + i).style("fill")).darker()});
            var tipx = d3.select("#rec" + i).attr("width");
            var tipy = margin.top + (barHeight * i) - 7;
            cputip.attr("x", tipx);
            cputip.attr("y", tipy);
            cputip.attr("dx", margin.left + 4);
            cputip.attr("dy", margin.top);
            cputip.style("visibility", "visible");
            cputip.style("fill", "gray");
            cputip.text(d.cpu + " CPU Hours");
            storagetip.attr("y", tipy);
            storagetip.attr("dx", margin.left + 4);
            storagetip.attr("dy", margin.top);
            storagetip.style("visibility", "visible");
            storagetip.style("fill", d3.rgb("white").brighter());
            storagetip.text((d.used/1000).toFixed(2) + " TB");})
        .on("mouseout", function(d, i){
            d3.select("#rec" + i).style("fill", function() { return d3.rgb(d3.select("#rec" + i).style("fill")).brighter()});
            d3.select("#bar" + i).style("fill", function() { return d3.rgb(d3.select("#bar" + i).style("fill")).brighter()});
            d3.select("#sub" + i).style("fill", function() { return d3.rgb(d3.select("#sub" + i).style("fill")).brighter()});
            cputip.style("visibility", "hidden");
            storagetip.style("visibility", "hidden");});

    site.append("text")
        .attr("x", margin.left - 6)
        .attr("dy", margin.top - 7)
        .style("fill", "gray")
        .style("font-weight", "bold")
        .style("text-anchor", "end")
        .text(function(d) { return d.site; });

    site.append("rect")
        .attr("id", function(d, i) { return "rec" + i })
        .attr("class", "rec")
        .attr("x", margin.left)
        .attr("width", barWidth)
        .attr("height", barHeight - 1)
        .style("fill", d3.rgb("gray").brighter());

    site.append("rect")
        .attr("id", function(d, i) { return "bar" + i })
        .attr("x", margin.left)
        .attr("width", function(d) { return x(d.used/d.quota); })
        .attr("height", barHeight-1)
        .style("fill", function(d) { return color(d.cpu/d.used); });

    site.append("rect")
        .attr("id", function(d, i) { return "sub" + i })
        .attr("x", function(d) { return margin.left + x(d.used/d.quota); })
        .attr("width", function(d) { return x(((d.used + d.subscribed)/d.quota) - (d.used/d.quota)); })
        .attr("height", barHeight-1)
        .style("fill", d3.rgb("black").brighter());

    site.append("text")
        .attr("x", function(d) { return margin.left + x(d.used/d.quota) - 4; })
        .attr("y", barHeight / 2)
        .attr("dy", ".35em")
        .text(function(d) {
            var percent = (d.used/d.quota)*100;
            return percent.toFixed(1) + "%"; });
});

function type(d) {
    d.site = d.site;
    d.quota = +d.quota;
    d.used = +d.used;
    d.cpu = +d.cpu;
    d.subscribed = +d.subscribed;
    return d;
}

