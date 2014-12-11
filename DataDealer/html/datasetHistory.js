var margin = {top: 100, right: 50, bottom: 50, left: 50};
var graphWidth = screen.width - margin.left - margin.right;
var graphHeight = screen.height - margin.top - margin.bottom;

var headers = ["CPU Hours", "Accesses", "CPU Threshold", "Acc Threshold"];
var ids = ["maxcpu", "maxacc", "dcpu", "dacc"]

var x = d3.scale.linear().domain([0, headers.length]).range([0, graphWidth]);
var yMaxCpu = d3.scale.linear().range([0, graphHeight]);
var yMaxAcc = d3.scale.linear().range([0, graphHeight]);
var yDeltaCpu = d3.scale.linear().range([0, graphHeight]);
var yDeltaAcc = d3.scale.linear().range([0, graphHeight]);

var chart = d3.select(".chart")
    .attr("width", graphWidth + margin.left + margin.right)
    .attr("height", graphHeight + margin.top + margin.bottom);

var graph = chart.append("g");

var axises = graph.selectAll("g")
    .data(headers)
    .enter().append("g")
        .attr("id", function(d, i) { return ids[i];});

axises.append("line")
    .attr("transform", function(d, i) { return "translate(" + (margin.left + x(i)) + ", " + margin.top + ")"; })
    .attr("x1", 0)
    .attr("y1", 0)
    .attr("x2", 0)
    .attr("y2", graphHeight)
    .attr("stroke-width", 3)
    .attr("stroke", "black");

axises.append("text")
    .attr("transform", function(d, i) { return "translate(" + (margin.left + x(i)) + ", " + margin.top + ")"; })
    .attr("dy", graphHeight + 15)
    .style("fill", "gray")
    .style("text-anchor", "middle")
    .text(function(d) { return d;});

axises.append("text")
    .attr("class", "maxVal")
    .attr("transform", function(d, i) { return "translate(" + (margin.left + x(i)) + ", " + margin.top + ")"; })
    .attr("dy", -15)
    .style("fill", "gray")
    .style("text-anchor", "middle");

d3.csv("datasets.csv", type, function(error, data) {
    var maxVals = [d3.max(data, function(d) { return d.maxCPU; }), d3.max(data, function(d) { return d.maxAcc; }), d3.max(data, function(d) { return d.deltaCPU; }), d3.max(data, function(d) { return d.deltaAcc; })];

    console.log(maxVals[0]);
    console.log(maxVals[1]);
    console.log(maxVals[2]);
    console.log(maxVals[3]);

    yMaxCpu.domain([0, function() { return maxVals[0];}]);
    yMaxAcc.domain([0, function() { return maxVals[1];}]);
    yDeltaCpu.domain([0, function() { return maxVals[2];}]);
    yDeltaAcc.domain([0, function() { return maxVals[3];}]);

    axises.selectAll(".maxVal")
        .data(headers)
        .text(function(d, i) { return maxVals[i];});

/*    var maxCpu = chart.select("#maxcpu")
        .data(data)
        .enter().append("g").append("line")
            .attr("x1", function() { return chart.select("#maxcpu").attr("x1");})
            .attr("y1", function(d) { return yMaxCpu(d.maxCPU);})
            .attr("x2", function() { return chart.select("#maxacc").attr("x1");})
            .attr("y2", function(d) { return yMaxAcc(d.maxAcc);})
            .attr("stroke-width", 1)
            .attr("stroke", "steelblue");*/
});

function type(d) {
    d.dataset = d.dataset;
    d.maxCPU = +d.maxCPU;
    d.deltaCPU = +d.deltaCPU;
    d.maxAcc = +d.maxAcc;
    d.deltaAcc = +d.deltaAcc;
    d.popularityTime = +d.popularityTime;
    d.dataTier = d.dataTier;
    d.sizeGb = +d.sizeGb;
    d.age = +d.age;
    return d;
}