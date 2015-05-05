var margin = {top: 30, right: 20, bottom: 40, left: 60};
var width = 4000;
// var width = screen.width;
var height = screen.height*0.70;

var x_scale = d3.scale.linear().range([margin.left, width - margin.left - margin.right]);
var y_scale = d3.scale.linear().range([height - margin.bottom, margin.top]);

var x_axis = d3.svg.axis()
    .scale(x_scale)
    .orient("bottom");

var y_axis = d3.svg.axis()
    .scale(y_scale)
    .orient("left");

var chart = d3.select(".chart")
    .attr("width", width)
    .attr("height", height);

d3.selection.prototype.moveToFront = function() {
    return this.each(function(){
        this.parentNode.appendChild(this);
    });
};

d3.csv("hd.csv", type, function(error, csv_data) {
    x_scale.domain([0, d3.max(csv_data, function(d) {return d.age;})]);
    y_scale.domain([0, d3.max(csv_data, function(d) { return d.accesses; })]);
    
    chart.append("g")
    .attr("class", "axis")
    .attr("transform", "translate(0," + (height - margin.bottom) + ")")
    .call(x_axis);

    chart.append("g")
    .attr("class", "axis")
    .attr("transform", "translate(" + margin.left + ", 0)")
    .call(y_axis);

    chart.append("text")
        .attr("transform", "translate(" + margin.left + ", " + margin.top + ") rotate(-90)")
        .attr("dy", 12)
        .style("text-anchor", "end")
        .style("font", "14px sans-serif")
        .style("fill", "black")
        .text("accesses");

    chart.append("text")
        .attr("x", width - margin.right - margin.left)
        .attr("y", height - margin.bottom)
        .attr("dy", -10)
        .style("text-anchor", "end")
        .style("font", "14px sans-serif")
        .style("fill", "black")
        .text("age (days)");

    var data = d3.nest()
        .key(function(d) {return d.dataset_name;})
        .entries(csv_data);

    var avg_data = d3.nest()
        .key(function(d) {return d.age;})
        .rollup(function(days) {return d3.mean(days, function(d) {return d.accesses;});})
        .entries(csv_data);

    var lineFunction = d3.svg.line()
        .x(function(d) {return x_scale(d.age);})
        .y(function(d) {return y_scale(d.accesses);})
        .interpolate("linear");

    var avgLineFunction = d3.svg.line()
        .x(function(d) {return x_scale(d.key);})
        .y(function(d) {return y_scale(d.values);})
        .interpolate("linear");

    chart.selectAll("path")
        .data(data)
        .enter().append("path")
        .attr("d", function(d) {return lineFunction(d.values);})
        .style("fill", "none")
        .style("stroke", "steelblue")
        .style("stroke-width", 1)
        .on("mouseover", function(d){
            d3.select(this)
                .style("stroke", "orange")
                .moveToFront();
            var pos = d3.mouse(this);
            d3.select(".chart")
                .append("text")
                    .attr("id", "info")
                    .attr("x", pos[0])
                    .attr("y", pos[1])
                    .attr("dx", 0)
                    .attr("dy", -15)
                    .text(d.key);
            })
        .on("mouseout", function(){
            d3.select(this).style("stroke", "steelblue");
            d3.select(".chart")
                .select("#info").remove();});

        chart.append("path")
            .attr("d", function() {return avgLineFunction(avg_data);})
            .style("fill", "none")
            .style("stroke", "red")
            .style("stroke-width", 2);
});

function type(d) {
    d.dataset_name = d.dataset_name;
    d.age = +d.age;
    d.accesses = +d.accesses;
    d.size_gb = +d.size_gb;
    d.data_tier = d.data_tier;
    return d;
}