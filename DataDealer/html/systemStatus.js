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

var tooltip = chart.append("text")
    .style("text-anchor", "start")
    .style("visibility", "hidden");

d3.tsv("system.tsv", type, function(error, data) {

    x.domain([0, 1]);
    color.domain([d3.max(data, function(d) { return d.cpu/d.used; }), d3.min(data, function(d) { return d.cpu/d.used; })]);

    chart.attr("height", margin.top + barHeight * data.length);

    var site = allgroup.selectAll("g")
        .data(data)
        .enter().append("g")
        .attr("transform", function(d, i) { return "translate(0, " + (margin.top + (i * barHeight)) + ")"; });

    site.append("text")
        .attr("x", margin.left - 6)
        .attr("dy", margin.top)
        .style("fill", "gray")
        .style("font-weight", "bold")
        .style("text-anchor", "end")
        .text(function(d) { return d.site; });

    site.append("rect")
        .attr("id", function(d, i) { return "rect" + i })
        .attr("x", margin.left)
        .attr("width", barWidth)
        .attr("height", barHeight - 1)
        .style("fill", d3.rgb("gray").brighter())
        .on("mouseover", function(d, i){
            d3.select("#bar" + i).style("fill", function() { return d3.rgb(d3.select("#bar" + i).style("fill")).darker()});
            d3.select(this).style("fill", function() { return d3.rgb(d3.select(this).style("fill")).darker()});
            var tipx = d3.select(this).attr("width");
            var tipy = margin.top + (barHeight * i);
            tooltip.attr("x", tipx);
            tooltip.attr("y", tipy);
            tooltip.attr("dx", margin.left + 4);
            tooltip.attr("dy", margin.top);
            tooltip.style("visibility", "visible");
            tooltip.style("fill", "gray");
            tooltip.text(d.cpu + " CPU Hours");})
        .on("mouseout", function(d, i){
            d3.select("#bar" + i).style("fill", function() { return d3.rgb(d3.select("#bar" + i).style("fill")).brighter()});
            d3.select(this).style("fill", function() { return d3.rgb(d3.select(this).style("fill")).brighter()});
            tooltip.style("visibility", "hidden");});

    site.append("rect")
        .attr("id", function(d, i) { return "bar" + i })
        .attr("x", margin.left)
        .attr("width", function(d) { return x(d.used/d.quota); })
        .attr("height", barHeight-1)
        .style("fill", function(d) { return color(d.cpu/d.used); })
        .on("mouseover", function(d, i){
            d3.select("#rect" + i).style("fill", function() { return d3.rgb(d3.select("#rect" + i).style("fill")).darker()});
            d3.select(this).style("fill", function() { return d3.rgb(d3.select(this).style("fill")).darker()});
            var tipx = d3.select("#rect" + i).attr("width");
            var tipy = margin.top + (barHeight * i);
            tooltip.attr("x", tipx);
            tooltip.attr("y", tipy);
            tooltip.attr("dx", margin.left + 4);
            tooltip.attr("dy", margin.top);
            tooltip.style("visibility", "visible");
            tooltip.style("fill", "gray");
            tooltip.text(d.cpu + " CPU Hours");})
        .on("mouseout", function(d, i){
            d3.select("#rect" + i).style("fill", function() { return d3.rgb(d3.select("#rect" + i).style("fill")).brighter()});
            d3.select(this).style("fill", function() { return d3.rgb(d3.select(this).style("fill")).brighter()});
            tooltip.style("visibility", "hidden");});

    site.append("text")
        .attr("x", function(d) { return margin.left + x(d.used/d.quota) - 4; })
        .attr("y", barHeight / 2)
        .attr("dy", ".35em")
        .text(function(d) {
            var percent = (d.used/d.quota)*100;
            return percent.toFixed(1) + "%"; })
        .on("mouseover", function(d, i){
            d3.select("#rect" + i).style("fill", function() { return d3.rgb(d3.select("#rect" + i).style("fill")).darker()});
            d3.select("#bar" + i).style("fill", function() { return d3.rgb(d3.select("#bar" + i).style("fill")).darker()});
            var tipx = d3.select("#rect" + i).attr("width");
            var tipy = margin.top + (barHeight * i);
            tooltip.attr("x", tipx);
            tooltip.attr("y", tipy);
            tooltip.attr("dx", margin.left + 4);
            tooltip.attr("dy", margin.top);
            tooltip.style("visibility", "visible");
            tooltip.style("fill", "gray");
            tooltip.text(d.cpu + " CPU Hours");})
        .on("mouseout", function(d, i){
            d3.select("#rect" + i).style("fill", function() { return d3.rgb(d3.select("#rect" + i).style("fill")).brighter()});
            d3.select("#bar" + i).style("fill", function() { return d3.rgb(d3.select("#bar" + i).style("fill")).brighter()});
            tooltip.style("visibility", "hidden");});
});

function type(d) {
    d.site = d.site;
    d.quota = +d.quota;
    d.used = +d.used;
    d.cpu = +d.cpu;
    return d;
}

