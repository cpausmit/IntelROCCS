var margin = {top: 100, right: 50, bottom: 50, left: 50};
var graph_width = screen.width - margin.left - margin.right;
var graph_height = screen.height - margin.top - margin.bottom - 150;

var chart = d3.select(".chart")
    .attr("width", graph_width + margin.left + margin.right)
    .attr("height", graph_height + margin.top + margin.bottom);

var graph = chart.append("g");

var x_scale = d3.scale.linear().range([0, graph_width]);
var y_scale = d3.scale.linear().range([0, graph_height]);

var x_axis = graph.selectAll("g")
    .data(["Accesses"])
    .enter().append("g");

var y_axis = graph.selectAll("g")
    .data(["Age"])
    .enter().append("g");

x_axis.append("line")
    .attr("x1", margin.left)
    .attr("y1", margin.top)
    .attr("x2", margin,.left)
    .attr("y2", graph_height)
    .attr("stroke-width", 3)
    .attr("stroke", "black");

y_axis.append("line")
    .attr("x1", margin.left)
    .attr("y1", graph_height)
    .attr("x2", graph_width)
    .attr("y2", graph_height)
    .attr("stroke-width", 3)
    .attr("stroke", "black");

x_axis.append("text")
    .attr("dy", graph_height + 15)
    .style("fill", "gray")
    .style("text-anchor", "middle")
    .text(function(d) {return d;});

y_axis.append("text")
    .attr("dx", graph_width + 15)
    .style("fill", "gray")
    .style("text-anchor", "start")
    .style("dominant-baseline", "central")
    .text(function(d) {return d;});

var min_size_gb_possible;
var max_size_gb_possible;
var data_tiers_available = new Array();
var min_age_possible;
var max_age_possible;
var min_size_gb;
var max_size_gb;
var data_tiers = new Array();
var min_age;
var max_age;

d3.csv("../../data/hua.csv", type, function(error, data) {
    set_init_vars(d3.min(data, function(d) {return d.size_gb;}), d3.max(data, function(d) {return d.size_gb;}), d3.map(data, function(d) {return d.data_tier;}).keys(), d3.min(data, function(d) {return d.age;}), d3.max(data, function(d) {return d.age;}));
    x_scale.domain(min_age, max_age);
    y_scale.domain(0, d3.max(data, function(d) { return d.accesses; }));
});

function set_init_vars(new_min_size_gb, new_max_size_gb, new_data_tiers, new_min_age, new_max_age) {
    min_size_gb_possible = new_min_size_gb;
    max_size_gb_possible = new_max_size_gb;
    data_tiers_available = new_data_tiers.slice(0);
    min_age_possible = new_min_age;
    max_age_possible = new_max_age;

    min_size_gb = min_size_gb_possible;
    max_size_gb = max_size_gb_possible;
    data_tiers = data_tiers_available;
    min_age = min_age_possible;
    max_age = max_age_possible;
}

function set_min_size(new_min_size_gb) {
    min_size_gb = new_min_size_gb;
}

function set_max_size(new_max_size_gb) {
    max_size_gb = new_max_size_gb;
}

function add_data_tier(new_data_tier) {
    data_tiers.push(new_data_tier);
}

function remove_data_tier(new_data_tier) {
    var i = data_tiers.indexOf(new_data_tier);
    if (i > -1) {
        data_tiers.splice(i, 1);
    }
}

function set_min_age(new_min_age) {
    min_age = new_min_age;
}

function set_max_age(new_max_age) {
    max_age = new_max_age;
}

function type(d) {
    d.dataset_name = d.dataset_name;
    d.age = +d.age;
    d.accesses = +d.accesses;
    d.size_gb = +d.size_gb;
    d.data_tier = d.data_tier;
    return d;
}