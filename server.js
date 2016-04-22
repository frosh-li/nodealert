var mysql      = require('mysql');
var bmsConn = mysql.createConnection({
  host     : 'localhost',
  user     : 'root',
  password : 'bmsroot',
  database : 'db_bms_english4'
});

var watchInter = 10000;

var conn = mysql.createConnection({
  host     : 'localhost',
  user     : 'root',
  password : 'bmsroot',
  database : 'qingdalinghang'
});
 
bmsConn.connect();

conn.connect();
 
/**

报警插入逻辑
1、清空tb_general_alarm表

*/

 
function watchBattery(){
	var category = 3; // 电池报警类型为3
	var alarmBuffer = new Buffer(1);
	alarmBuffer.writeInt8(1);
	return new Promise(function(resolve, reject){
		bmsConn.query('delete from tb_general_alarm', function(err, rows, fields) {
		  if (err) throw err;
		  //console.log('The solution is: ', rows, fields);
		  return resolve({
		  	rows:rows,
		  	fields: fields
		  });
		})
	}).then(function(){
		return new Promise(function(resolve, reject){
			bmsConn.query('select * from tb_battery_module where Status1 > 0', function(err, rows, fields) {
			  if (err) throw err;
			  //console.log('The solution is: ', rows, fields);
			  return resolve({
			  	rows:rows,
			  	fields: fields
			  });
			})
		})
	}).then(function(ret){
		var querys = [];
		var rows = ret.rows;
		rows.map((row, i)=>{
			var sid = row.sid;
			var codes = [];
			ret.fields.map((field)=>{
				//console.log(field.name);
				console.log(field.name, row[field.name], alarmBuffer);

				if(row[field.name] == '\u0001'){
					codes.push("'"+field.name+"'");
				}
			})
			if(codes.length == 0){
				console.log('no codes')
				return;
			}
			console.log(codes);
			querys.push(new Promise(function(resolve, reject){
				conn.query(`select * from my_alarm_siteconf where category = ${category} and alarm in (${codes.join(",")})`, function(err, data){
					if(err){
						console.log(err);
						return reject(err);
					}
					//console.log('alarm datas:-------------------------------')
					//console.log(data);
					return resolve({
						ret:row,
						alarm: data
					})
				});
			}));
		});
		//return new Promise(function(resolve, reject){
		return Promise.all(querys)
		//});
	}).then(function(data){
		console.log(data)
		var inserts = [];

		data.map((ret)=>{
			ret.alarm.map((_alarm, i)=>{
				var obj = {
					//alarm_sn: +new Date(),
					alram_equipment: ret.ret.sn_key,
					equipment_sn: ret.ret.sn_key.toString().substr(0,10),
					alarm_code: _alarm.alarm_code,
					alarm_content: _alarm.content,
					alarm_emergency_level: _alarm.alarm_type,
					alarm_suggestion: _alarm.suggest,
					alarm_para1_name: ret.ret.sn_key.toString().substr(0,10),
					alarm_para1_value: "",
					alarm_para2_name: ret.ret.sn_key.toString().substr(0,12),
					alarm_para2_value: "",
					alarm_para3_name: ret.ret.sn_key,
					alarm_para2_value: "",
					//alarm_occur_time: _alarm.create_time,
					//alarm_recovered_time: _alarm.update_time,
					//alarm_update_time: _alarm.create_time
				}
				inserts.push(new Promise(function(resolve, reject){
					bmsConn.query('insert into tb_general_alarm set ?', obj, function(err, results){
						if(err){
							console.log(err);
							return reject(err);
						}
						return resolve({
							status: 200
						})
					});
				}));
			})
			
		});
		console.log(inserts);
		return Promise.all(inserts,function(values){
			console.log(values);
		});

	}).then(function(){
		setTimeout(watchBattery, watchInter);
	}).catch(function(e){
		setTimeout(watchBattery, watchInter);
		console.log(e);
	});
}

watchBattery();
