const camelize = require('camelcase');
const decamelize = require('decamelize');
const Fisher = require('fisherjs');

module.exports = ({_dbFlavor,_emitter,_className,_table,_columns}) => {
  _emitter = _emitter || new Fisher();
  _table = _table || 'table';
  _className = _className || camelize(_table,{pascalCase: true});
  _table = '`'+_table+'`';
  _dbFlavor = _dbFlavor|| 'mysql';
  let _sqlColumns = _columns.map(value => '`'+decamelize(value.name)+'`');
  let _objProperties = _columns.map(value => camelize(value.name));
  let _sqlPrimaryKey = _columns.filter(value => value.primaryKey).map(value => '`'+decamelize(value.name)+'`');
  let _objPrimaryKey = _columns.filter(value => value.primaryKey).map(value => camelize(value.name));
  let _autoIncrement = _columns.filter(value => value.primaryKey && value.autoIncrement).length>0;

  if(_autoIncrement && _objPrimaryKey.length>1)
    console.error("Auto increment works only with simple a primary key.");

  let _classNameCapit = _className[0].toUpperCase() + _className.slice(1)

  // helper function
  function _isSet(o){
    return typeof o !== 'undefined';
  }

  // helper function
  function _camelizeObject(o){
    let co = {}
    // The objects are rows, so they are shallow. Go deep will cause problems with Date objects.
    // Object.keys(o).forEach(prop => {co[camelize(prop)] = (typeof o[prop] === 'object')?_camelizeObject(o[prop]):o[prop];});
    Object.keys(o).forEach(prop => {co[camelize(prop)] = o[prop]});
    return co;
  }

  // Small hack to create a dymanic named class
  let namedClass = {[_className]: class {
    constructor(o) {
      Object.assign(this,o);
    }

    static get table(){
      return _table;
    }

    static get className(){
      return _className;
    }

    static createTable(conn){
      let cols = _columns.map((col,i) => `
        ${_sqlColumns[i]} ${col.type}\
        ${_sqlPrimaryKey.length == 1 && col.primaryKey?' PRIMARY KEY':''}\
        ${_sqlPrimaryKey.length == 1 && col.primaryKey && col.autoIncrement?(_dbFlavor=='mysql'?' AUTO_INCREMENT':' AUTOINCREMENT'):''}\
        ${col.constraint? ' '+col.constraint:''}\
      `);
      let foreignKeys = _dbFlavor!='mysql'? []:_columns.filter(col => col.foreignKey).map(col => `
        FOREIGN KEY (${decamelize(col.name)})\
        REFERENCES ${col.foreignKey.references}\
        ON DELETE ${col.foreignKey.onDelete}\
        ON UPDATE ${col.foreignKey.onUpdate}\
      `);
      let indexes = _dbFlavor!='mysql'? []:_columns.filter(col => col.index).reduce(function(idxs,col){
        if(!_isSet(idxs[col.index])) idxs[col.index] = [];
        idxs[col.index].push(decamelize(col.name));
        return idxs;
      },[]);

      let sqlIndexes = [];
      Object.keys(indexes).forEach(idxName => {
        sqlIndexes.push(`INDEX ${idxName} (${indexes[idxName].join(',')})`)
      })
      let query = `
        CREATE TABLE IF NOT EXISTS ${_table} (
          ${cols.join(',')}\
          ${_sqlPrimaryKey.length > 1 ? ',PRIMARY KEY (' + _sqlPrimaryKey.join(',') + ')' : ''}
          ${foreignKeys.length>0 ? ','+foreignKeys.join(','):''}
          ${sqlIndexes.length>0 ? ','+sqlIndexes.join(','):''}
        ) ${_dbFlavor=='mysql'?'ENGINE INNODB':''};
      `;
      return this.rawRun(conn,query,[]);
    }

    static dropTable(conn){
      let query = `DROP TABLE IF EXISTS ${_table};`;
      return this.rawRun(conn,query,[]);
    }

    // nice public helper functions
    static timestampToDatetime(t){
      return new Date(t).toISOString().slice(0,19).replace('T',' ')
    }
    static timestampToLocalDatetime(t){
      let tzoffset = new Date().getTimezoneOffset()*60000;
      return this.timestampToDatetime(t - tzoffset);
    }
    static now(){
      return this.timestampToLocalDatetime(Date.now());
    }
    static getSQLColumns() {
      return _table+'.'+_sqlColumns.join(','+_table+'.');
    }

    // CRUD stuff
    async save(conn){
      await _emitter.emit('entityPreSave'+_classNameCapit,conn,this);
      let create = _objPrimaryKey.reduce((a,key) => !_isSet(this[key]) && a,true)
      if(create)
        return await this.create(conn);
      return await this.update(conn);
    }

    async create(conn){
      await _emitter.emit('entityPreCreate'+_classNameCapit,conn,this);
      let cols = _sqlColumns.filter((col,i) => _isSet(this[_objProperties[i]]));
      let values = _objProperties.filter(prop => _isSet(this[prop])).map(prop => this[prop]);
      const query = `
        INSERT INTO ${_table} (${cols.join(',')})
        VALUES (${Array(cols.length).fill('?').join(',')});
      `;
      let id = await this.constructor.rawInsert(conn,query,values);
      if(_autoIncrement)
        this[_objPrimaryKey[0]]=id;
      await _emitter.emit('entityCreate'+_classNameCapit,conn,this);
      await _emitter.emit('entityLoad'+_classNameCapit,conn,this);
      return this;
    }

    async update(conn){
      await _emitter.emit('entityPreUpdate'+_classNameCapit,conn,this);
      let cols = _sqlColumns.filter((col,i) => !_sqlPrimaryKey.includes(col) && _isSet(this[_objProperties[i]])).map(col => col+' = ?');
      let values = _objProperties.filter(prop => !_objPrimaryKey.includes(prop) && _isSet(this[prop])).map(prop => this[prop]);
      let where = _sqlPrimaryKey.map(pk => pk+' = ?');
      values = values.concat(_objPrimaryKey.map(prop => this[prop]));
      const query = `UPDATE ${_table} SET ${cols.join(',')} WHERE ${where.join(' AND ')};`;
      await this.constructor.rawUpdate(conn,query,values);
      await _emitter.emit('entityUpdate'+_classNameCapit,conn,this);
      await _emitter.emit('entityLoad'+_classNameCapit,conn,this);
      return this;
    }

    async load(conn){
      let where = _sqlPrimaryKey.map(pk => pk+' = ?');
      let values = _objPrimaryKey.map(prop => this[prop]);
      let query = `SELECT * FROM ${_table} WHERE ${where.join(' AND ')};`;
      let rows = await this.constructor.rawAll(conn,query,values);
      if(rows.length==0) throw 404;
      Object.assign(this,_camelizeObject(rows[0]));
      await _emitter.emit('entityLoad'+_classNameCapit,conn,this);
      return this;
    }

    async delete(conn){
      await _emitter.emit('entityPredelete'+_classNameCapit,conn,this);
      let where = _sqlPrimaryKey.map(pk => pk+' = ?');
      let values = _objPrimaryKey.map(prop => this[prop]);
      let query = `DELETE FROM ${_table} WHERE ${where.join(' AND ')};`;
      await this.constructor.rawDelete(conn,query,values);
      await _emitter.emit('entityDelete'+_classNameCapit,conn,this);
      return this;
    }

    async prepare(conn){
      await _emitter.emit('entityPrepare'+_classNameCapit,conn,this);
      return this;
    }

    // static basic raw methods (do not emit events)
    static rawAll(conn,query,values){
      return new Promise((resolve,reject) => {
        if(_dbFlavor=='mysql'){
          conn.query(query,values,function(err,rows){
            if(err) reject(err);
            resolve(rows)
          });
        } else {
          conn.all(query,values,function(err,rows){
            if(err) reject(err);
            resolve(rows)
          });
        }
      });
    }

    static rawRun(conn,query,values){
      return new Promise((resolve,reject) => {
        if(_dbFlavor=='mysql'){
          conn.query(query,values,function(err,ret){
            if(err) reject(err);
            resolve(ret)
          });
        } else {
          conn.run(query,values,function(err){
            if(err) reject(err);
            resolve({insertId:this.lastID,changedRows:this.changes,affectedRows:this.changes})
          });
        }
      });
    }

    static rawInsert(conn,query,values){
      return this.rawRun(conn,query,values).then(ret => ret.insertId);
    }

    static rawUpdate(conn,query,values){
      return this.rawRun(conn,query,values).then(ret => ret.changedRows);
    }

    static rawDelete(conn,query,values){
      return this.rawRun(conn,query,values).then(ret => ret.affectedRows);
    }

    // class aware static methods (emit events)
    static runSelect(conn,query,values){
      return this.rawAll(conn,query,values).then(rows => {
        let results = [];
        rows.forEach((row) => {
          let e = new namedClass[_className](_camelizeObject(row));
          _emitter.emit('entityLoad'+_classNameCapit,conn,e);
          results.push(e);
        });
        _emitter.emit('entityLoadMultiple'+_classNameCapit,conn,results);
        return results;
      })
    }

    static list(conn,offset,limit){
      return this.runSelect(conn,`SELECT * FROM ${_table} LIMIT ${offset},${limit}`,[]);
    }

    static search(conn,values,offset,limit,fuzzy=false,op='AND',whereInjection='',whereValuesInjection=[],selectInjection='',joinInjection=''){
      let cols = _sqlColumns.filter((col,i) => _isSet(values[_objProperties[i]]));

      if(!Array.isArray(fuzzy)) fuzzy = [fuzzy];
      while(fuzzy.length<cols.length)
        fuzzy.push(fuzzy[fuzzy.length-1]);

      if(!Array.isArray(op)) op = [op];
      while(op.length<cols.length-1)
        fuzzy.push(op[op.length-1]);

      let where = cols.map((col,i) => col+(fuzzy[i]?' LIKE ?':' = ?')).reduce((w,curr,i) => `${w} ${op[i-1]} ${curr}`);
      values = _objProperties.filter(prop => _isSet(values[prop])).map(prop => values[prop]);
      const query = `
        SELECT ${this.getSQLColumns()} ${selectInjection!=''?','+selectInjection:''}
        FROM ${_table}
        ${joinInjection}
        WHERE ${where} ${whereInjection!=''? 'AND ('+whereInjection+')': ''}
        LIMIT ${offset},${limit}
      ;`;
      return this.runSelect(conn,query,values.concat(whereValuesInjection));
    }
  }}
  return namedClass[_className];
}