// Set up interface
var margin = {top: 30, right: 20, bottom: 40, left: 60};
// var width = 4000;
var width = screen.width;
var height = screen.height*0.70;
var radius = 5;

var x_scale = d3.time.scale().range([margin.left, width - margin.left - margin.right]);
var y_scale = d3.scale.linear().range([height - margin.bottom, margin.top]);

var x_axis = d3.svg.axis()
    .scale(x_scale)
    .orient("bottom")
    .tickFormat(d3.time.format("%Y-%m-%d"));

var y_axis = d3.svg.axis()
    .scale(y_scale)
    .orient("left")
    .ticks(11);

var class_x = margin.left;
var class_type = "unchanged";

var chart = d3.select(".chart")
    .attr("width", width)
    .attr("height", height)
    .on("click", function(d) {
        var coordinates = [0, 0];
        coordinates = d3.mouse(this);
        class_x = coordinates[0];
        d3.selectAll(".classify")
            .attr("disabled", null);
    });

var cpu_line = d3.svg.line()
    .x(function(d) { return x_scale(d.date); })
    .y(function(d) { return y_scale(d.norm_n_cpus); });

var access_line = d3.svg.line()
    .x(function(d) { return x_scale(d.date); })
    .y(function(d) { return y_scale(d.norm_n_accesses); });

var date_parser = d3.time.format("%Y-%m-%d").parse;
var dataset = 0;
var classifications = [];
var dl_file = null;

d3.json("./data_visualization.json", function(error, raw_data) {
    // Parse data
    data = [];
    raw_data.forEach(function(entry) {
        dataset_name = entry.dataset_name;
        size_gb = +entry.size_gb;
        n_files = +entry.n_files;
        physics_group = entry.physics_group;
        ds_type = entry.ds_type;
        data_tier = entry.data_tier;
        max_cpu = d3.max(entry.popularity, function(d) { return d.n_cpus; });
        max_access = d3.max(entry.popularity, function(d) { return d.n_accesses; });
        popularity = [];
        entry.popularity.forEach(function(pop_entry) {
            date = date_parser(pop_entry.date);
            n_cpus = +pop_entry.n_cpus;
            n_accesses = +pop_entry.n_accesses;
            popolarity_data = {
                "date": date,
                "n_cpus": n_cpus,
                "n_accesses": n_accesses,
                "norm_n_cpus": n_cpus/max_cpu,
                "norm_n_accesses": n_accesses/max_access
            };
            popularity.push(popolarity_data);
        });
        dataset_data = {
            "dataset_name": dataset_name,
            "popularity": popularity
        };
        data.push(dataset_data);
    });

    chart.append("g")
        .attr("id", "x_axis")
        .attr("class", "axis")
        .attr("transform", "translate(0," + (height - margin.bottom) + ")")
        .call(x_axis);

    chart.append("g")
        .attr("id", "y_axis")
        .attr("class", "axis")
        .attr("transform", "translate(" + margin.left + ", 0)")
        .call(y_axis);

    chart.append("text")
        .attr("transform", "translate(" + margin.left + ", " + margin.top + ") rotate(-90)")
        .attr("dy", 12)
        .style("text-anchor", "end")
        .text("Normalized CPU and Accesses");

    chart.append("text")
        .attr("x", width - margin.right - margin.left)
        .attr("y", height - margin.bottom)
        .attr("dy", -10)
        .style("text-anchor", "end")
        .text("Date");

    chart.append("path")
        .attr("id", "cpu_line")
        .attr("class", "line");
    chart.append("path")
        .attr("id", "access_line")
        .attr("class", "line");

    chart.append("text")
        .attr("id", "dataset_name")
        .attr("x", margin.left)
        .attr("y", margin.top)
        .attr("dy", 15)
        .attr("dx", 30)
        .style("text-anchor", "start")
        .text("");

    plot_dataset(dataset);
});

function plot_dataset(dataset_num) {
    var chart = d3.select("body").transition();

    dataset_data = data[dataset_num];
    x_axis.ticks(d3.time.day, 7);
    x_scale.domain([d3.min(dataset_data.popularity, function(d) { return d.date; }), d3.max(dataset_data.popularity, function(d) { return d.date; })]);
    y_scale.domain([0, 1]);

    max_cpu = d3.max(dataset_data.popularity, function(d) { return d.n_cpus; });
    max_access = d3.max(dataset_data.popularity, function(d) { return d.n_accesses; });

    var dataset_text = '#' + dataset_num + ' : ' + dataset_data.dataset_name + ' | Max Accesses: ' + max_access + ' | Max CPU: ' + max_cpu;

    chart.select("#cpu_line")
        .duration(750)
        .attr("d", cpu_line(dataset_data.popularity));
    chart.select("#access_line")
        .duration(750)
        .attr("d", access_line(dataset_data.popularity));
    chart.select("#x_axis")
        .duration(750)
        .call(x_axis);
    chart.select("#y_axis")
        .duration(750)
        .call(y_axis);
    chart.select("#dataset_name")
        .text(dataset_text);
}

function make_file() {
    var json = JSON.stringify(classifications);
    var blob = new Blob([json], {type: "application/json"});
    if (dl_file !== null) {
        window.URL.revokeObjectURL(dl_file);
    }
    dl_file = window.URL.createObjectURL(blob);
    return dl_file;
}

function set_class_type() {
    class_type = d3.select("#class_type").node().value;
}

function add_classification() {
    var date = x_scale.invert(class_x);
    class_data = {
        "dataset_name": dataset_data.dataset_name,
        "date": date,
        "classification": class_type
    };
    classifications.push(class_data);

    chart.append("circle")
        .attr("class", "classification")
        .attr("cx", function (d) { return x_scale(date); })
        .attr("cy", function (d) { return y_scale(0); })
        .attr("r", radius);

    d3.selectAll(".classify")
        .attr("disabled", "disabled");
}

function remove_calssifications() {
    chart.selectAll(".classification")
        .remove();
}

function next_dataset() {
    if (dataset < (data.length - 1)) {
        dataset++;
        plot_dataset(dataset);
    }
    remove_calssifications();
}

function submit_classification() {
    var link = d3.select("#downloadlink");
    link.attr("href", make_file());
    link.style("display", "inline");
}
