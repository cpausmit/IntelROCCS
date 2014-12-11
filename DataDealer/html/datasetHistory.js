var margin = {top: 100, right: 50, bottom: 50, left: 50};
var graphWidth = screen.width - margin.left - margin.right;
var graphHeight = screen.height - margin.top - margin.bottom;

var headers = ["CPU Hours", "Accesses", "CPU Threshold", "Acc Threshold"];

var x = d3.scale.linear().domain([0, headers.length]).range([0, graphWidth]);

var chart = d3.select(".chart")
    .attr("width", graphWidth + margin.left + margin.right)
    .attr("height", graphHeight + margin.top + margin.bottom);

var allAxises = chart.append("g");

allAxises.selectAll("g")
    .data(headers)
    .enter().append("line")
    .attr("transform", function(d, i) { return "translate(" + (margin.left + x(i)) + ", " + margin.top + ")"; })
    .attr("x1", 0)
    .attr("y1", 0)
    .attr("x2", 0)
    .attr("y2", graphHeight)
    .attr("stroke-width", 2)
    .attr("stroke", "black");

allAxises.selectAll("g")
    .data(headers)
    .enter().append("text")
    .attr("transform", function(d, i) { return "translate(" + (margin.left + x(i)) + ", " + margin.top + ")"; })
    .attr("dy", -15)
    .style("fill", "gray")
    .style("text-anchor", "middle")
    .text(function(d) { return d;})

d3.csv("datasets.csv", type, function(error, data) {
    .data(data)
    .enter().append("g")
});

function type(d) {
    d.dataset = d.dataset;
    d.deltaCPU = +d.deltaCPU;
    d.maxAcc = +d.maxAcc;
    d.deltaAcc = +d.deltaAcc;
    d.popularityTime = +d.popularityTime;
    d.dataTier = d.dataTier;
    d.sizeGb = +d.sizeGb;
    d.age = +d.age;
    return d;
}