  var AWS = require('aws-sdk');
  AWS.config.loadFromPath('./config.json');

  var db = new AWS.DynamoDB();

  function keyvaluestore(table) {
    this.LRU = require("lru-cache");
    this.cache = this.LRU({ max: 500 });
    this.tableName = table;
  };

  /**
   * Initialize the tables
   * 
   */
  keyvaluestore.prototype.init = function(whendone) {
    
    var tableName = this.tableName;
    var self = this;
    
    var params = {
      TableName: tableName /* required */
    };
    
    db.waitFor('tableExists', params, function(err, data) {
      if (err){
        console.log(err, err.stack); // an error occurred
      }
      else{
        whendone(); //Call Callback function.
      }   
    });

  };

  /**
   * Get result(s) by key
   * 
   * @param search
   * 
   * Callback returns a list of objects with keys "inx" and "value"
   */
  
keyvaluestore.prototype.get = function(search, callback) {
    var self = this;
    
    if (self.cache.get(search))
          callback(null, self.cache.get(search));
    else {
        
      //define params
      var params = {
        ExpressionAttributeValues: {":k1": {S: search}}, 
        KeyConditionExpression: "keyword = :k1", 
        TableName: this.tableName
       };

       //do query to get elements from dynamodb
      db.query(params, function(err, data) {
        if(err) {
          callback(err,null);
        } else {

          //map items to match the expected format for callback
          var items = [];
          if(data.Count > 0){
            data.Items.forEach(function(item){
              items.push({inx : item.inx.N, value : item.value.S, key : item.keyword.S});
            });
            callback(false,items);
          } else {
            callback(true, null);
          }
        }
      });
      
    }
  };

  module.exports = keyvaluestore;
