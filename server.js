var mysql      = require('mysql');
var bmsConn = mysql.createConnection({
  host     : 'localhost',
  user     : 'root',
  password : 'bmsroot',
  database : 'db_bms_english4'
});

var watchInter = 30000;

 
bmsConn.connect();

function BParse(conf, battery){
	return {
		record_time:new Date(),
		alarm_sn: battery.sn_key,
		alram_equipment: battery.site_name || "未分配站点",
		equipment_sn: battery.sn_key.toString().substr(0,10),
		alarm_code: conf.alarm_code,
		alarm_content: conf.content,
		alarm_emergency_level: conf.alarm_type,
		alarm_suggestion: conf.suggest,
		alarm_para1_name: battery.sid,
		alarm_para1_value: battery[conf.operator],
		alarm_para1_newdata:battery[conf.operator],
		alarm_para2_name: battery.gid||"",
		alarm_para2_value: battery[conf.operator],
		alarm_para2_newdata: battery[conf.operator],
		alarm_para3_name: battery.mid||"",
		alarm_para3_newdata: battery[conf.operator],
		alarm_para3_value: battery[conf.operator],
		alarm_sms_send_time:new Date(),
		alarm_email_send_time: new Date(),
		alarm_proces_reaction:'成功',
		alarm_decide_type:"tb_battery_module",

		alarm_occur_time: new Date(),
		alarm_recovered_time: new Date(),
		alarm_update_time: new Date(),
		alarm_process_and_memo:'',
		alarm_process_people:"",
		alarm_process_time:new Date(),
		alarm_light_excuted:0,
		alarm_memo:"",
	}
}

 
/**

报警插入逻辑
1、清空tb_general_alarm表

*/

 
function watchBattery(){
	var category = 3; // 电池报警类型为3
	var alarmBuffer = new Buffer(1);
	alarmBuffer.writeInt8(1);
	return new Promise(function(resolve, reject){
		bmsConn.query('insert into tb_general_alarm_history (select * from tb_general_alarm)', function(err, rows, fields) {
		  if (err) throw err;
		  //console.log('The solution is: ', rows, fields);
		  return resolve({
		  	rows:rows,
		  	fields: fields
		  });
		})
	}).then(function(){
		return new Promise(function(resolve, reject){
			bmsConn.query('delete from tb_general_alarm', function(err, rows, fields) {
			  if (err) throw err;
			  //console.log('The solution is: ', rows, fields);
			  return resolve({
			  	rows:rows,
			  	fields: fields
			  });
			})
		})
	}).then(function(){
		return new Promise(function(resolve, reject){
			bmsConn.query("select * from my_alarm_siteconf as conf where conf.category = 3 AND conf.type_value LIKE '{%}%' ", function(err, rows, fields) {
			  if (err) throw err;
			  
			  return resolve(rows);
			})
		})
	
	}).then(function(confRows){
		var alarms = [];
		return new Promise(function(resolve, reject){
			bmsConn.query('select tb_battery_module.*,my_site.site_name from tb_battery_module left join my_site on (my_site.sid = tb_battery_module.sid)', function(err, rows, fields) {
			  if (err) throw err;
			  //直接开始处理具体数据
			  rows.map((battery)=>{
			  	confRows.map((conf)=>{
			  		var operator = conf.type_value.split("and");
			  		operator.map((o)=>{
			  			var kv = o.split(/(>=|<=|>|<)/);
			  			switch(kv[1]){
			  				case ">=" :
			  					if(battery[conf.operator] >= kv[2] && battery.sid == conf.sid){
			  						alarms.push(BParse(conf,battery))
			  					}
			  					break;
			  				case "<=" :
			  					if(battery[conf.operator] <= kv[2] && battery.sid == conf.sid){
			  						alarms.push(BParse(conf,battery))
			  					}
			  					break;
							case ">" :
			  					if(battery[conf.operator] > kv[2] && battery.sid == conf.sid){
			  						alarms.push(BParse(conf,battery))
			  					}
			  					break;
			  				case "<" :
			  					if(battery[conf.operator]< kv[2] && battery.sid == conf.sid){
			  						alarms.push(BParse(conf,battery))
			  					}
			  					break;			  					
			  			}
			  		})
			  	})
			  })
			  resolve({
			  	alarms:alarms,
			  	confRows:confRows
			  })
			})
		})
	}).then(function(data){
		var alarms = data.alarms;
		var confRows = data.confRows;
		return new Promise(function(resolve, reject){
			bmsConn.query('select tb_station_module.*,my_site.site_name from tb_station_module left join my_site on (my_site.sid = tb_station_module.sid)', function(err, rows, fields) {
			  if (err) throw err;
			  //直接开始处理具体数据
			  rows.map((battery)=>{
			  	confRows.map((conf)=>{
			  		var operator = conf.type_value.split("and");
			  		operator.map((o)=>{
			  			var kv = o.split(/(>=|<=|>|<)/);
			  			switch(kv[1]){
			  				case ">=" :
			  					if(battery[conf.operator] >= kv[2] && battery.sid == conf.sid){
			  						alarms.push(BParse(conf,battery))
			  					}
			  					break;
			  				case "<=" :
			  					if(battery[conf.operator] <= kv[2] && battery.sid == conf.sid){
			  						alarms.push(BParse(conf,battery))
			  					}
			  					break;
							case ">" :
			  					if(battery[conf.operator] > kv[2] && battery.sid == conf.sid){
			  						alarms.push(BParse(conf,battery))
			  					}
			  					break;
			  				case "<" :
			  					if(battery[conf.operator]< kv[2] && battery.sid == conf.sid){
			  						alarms.push(BParse(conf,battery))
			  					}
			  					break;			  					
			  			}
			  		})
			  	})
			  })
			  resolve({
			  	alarms:alarms,
			  	confRows:confRows
			  })
			})
		})
	}).then(function(data){
		var alarms = data.alarms;
		var confRows = data.confRows;
		return new Promise(function(resolve, reject){
			bmsConn.query('select tb_group_module.*,my_site.site_name from tb_group_module left join my_site on (my_site.sid = tb_group_module.sid)', function(err, rows, fields) {
			  if (err) throw err;
			  //直接开始处理具体数据
			  rows.map((battery)=>{
			  	confRows.map((conf)=>{
			  		var operator = conf.type_value.split("and");
			  		operator.map((o)=>{
			  			var kv = o.split(/(>=|<=|>|<)/);
			  			switch(kv[1]){
			  				case ">=" :
			  					if(battery[conf.operator] >= kv[2] && battery.sid == conf.sid){
			  						alarms.push(BParse(conf,battery))
			  					}
			  					break;
			  				case "<=" :
			  					if(battery[conf.operator] <= kv[2] && battery.sid == conf.sid){
			  						alarms.push(BParse(conf,battery))
			  					}
			  					break;
							case ">" :
			  					if(battery[conf.operator] > kv[2] && battery.sid == conf.sid){
			  						alarms.push(BParse(conf,battery))
			  					}
			  					break;
			  				case "<" :
			  					if(battery[conf.operator]< kv[2] && battery.sid == conf.sid){
			  						alarms.push(BParse(conf,battery))
			  					}
			  					break;			  					
			  			}
			  		})
			  	})
			  })
			  resolve(alarms)
			})
		})
	}).then(function(data){
		var inserts = [];

		data.map((obj)=>{

			
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
