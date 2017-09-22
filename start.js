// var data_stream = require("./git-event-gen");
var spawn = require("child_process").spawn;
var py_script = spawn('python',["/Users/daichi/Desktop/dev/hackcapital/clean.py"]);


py_script.stdout.on('data', function (data){
	console.log("Start of Process")
	console.log("stdout.on.data: " + data)
});

py_script.stderr.on('data', function (data){
	console.log("Error: " + data)
});

py_script.stdout.on('end', function(){
   console.log("End of Process");
});