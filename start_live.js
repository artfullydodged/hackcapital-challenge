var fs = require('fs');
var read_stream = fs.createReadStream('streaming_git.log');
var async = require('async');
var parallelLimit = 1;
var run_type = "live"

function clean(data, cb) {  
	var spawn = require("child_process").spawn;
	var py_script = spawn('python',["/Users/daichi/Desktop/dev/hackcapital/clean.py", run_type, data]);

	py_script.stdout.on('data', function (d){
		console.log("stdout.on.data: " + d)
	});

	py_script.stderr.on('data', function (d){
		console.log("Error: " + d)
	});

	py_script.stdout.on('end', function(){
	   console.log("End of Process");
	});
}

var q = async.queue(clean, parallelLimit);

function push(data, cb) {  
	q.push(data, cb);
};


read_stream.setEncoding('utf8');

read_stream.on('data', function(chunk) {
    push(chunk);
});

