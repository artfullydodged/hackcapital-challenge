var spawn = require("child_process").spawn;
var run_type = "static"
var py_script = spawn('python',["/Users/daichi/Desktop/dev/hackcapital/clean.py", run_type]);


py_script.stdout.on('data', function (data){
	console.log("stdout.on.data: " + data)
});

py_script.stderr.on('data', function (data){
	console.log("Error: " + data)
});

py_script.stdout.on('end', function(){
   console.log("End of Process");
});