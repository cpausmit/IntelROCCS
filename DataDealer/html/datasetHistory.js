var margin = {top: 30, right: 20, bottom: 20, left: 30};
var height = 500 - margin.bottom - margin.top;
var barWidth = 20;

var y = d3.scale.linear().range([0, height]);

var chart = d3.select(".chart")
        .attr("height", height + margin.bottom + margin.top);

var allgroup = chart.append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var tooltip = chart.append("text")
        .style("visibility", "hidden");

d3.tsv("datasetHistory.tsv", type, function(error, data) {

	y.domain([0, d3.max(data, function(d) { return d.cpuh; })]);

	chart.attr("width", margin.left + barWidth * data.length);

	var bar = allgroup.selectAll("g")
		.data(data)
		.enter().append("g")
		.attr("transform", function(d, i) { return "translate(" + i * barWidth + ", 0)"; });

	bar.append("rect")
        .attr("y", function(d) { return height - y(d.cpuh); })
        .attr("height", function(d) { return y(d.cpuh); })
        .attr("width", barWidth - 1)
        .on("mouseover", function(d, i){
            var tipx = (barWidth) * i + margin.left + margin.right - 1;
            var tipy = height - d3.select(this).attr("height");
            tooltip.attr("x", tipx);
            tooltip.attr("y", tipy);
            tooltip.attr("dx", (barWidth-1)/2);
            tooltip.attr("dy", 25);
            tooltip.style("text-anchor", "center")
            tooltip.style("visibility", "visible");
            tooltip.style("fill", "black");
            tooltip.text(d.cpuh);
            d3.select(this).style("fill", "orange");})
        .on("mouseout", function(){
            tooltip.style("visibility", "hidden");
            d3.select(this).style("fill", "steelblue");});
});

function type(d) {
	d.cpuh = +d.cpuh;
    d.date = d.date;
	return d;
}

