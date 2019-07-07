const Notorm = require('../');
const sqlite3 = require('sqlite3').verbose();
const mysql = require('mysql');

async function run(){
  try{
    console.log("MySQL Connection");
    let pool = mysql.createPool({
      connectionLimit: 10,
      host: "localhost",
      database: "notorm",
      user: "root",
      password: "root"
    });
    let conn = await new Promise((resolve, reject) => {
      pool.getConnection(function(err,conn) {
        if(err) reject(err);
        resolve(conn);
      });
    });

    console.log("MySQL Data Definition");
    Post = Notorm({_table:'post',_columns:[
      {name:'id',type:'INT(11)',primaryKey:true,autoIncrement:true},
      {name:'title',type:'VARCHAR(255)'},
      {name:'body',type:'TEXT'},
      {name:'author',type:'VARCHAR(255)'},
      {name:'date',type:'DATETIME'}
    ]})
    Comment = Notorm({_table:'comment',_columns:[
      {name:'id',type:'INT(11)',primaryKey:true,autoIncrement:true},
      {name:'postId',type:'INT(11)',foreignKey:{references:'post(id)',onDelete:'CASCADE',onUpdate:'CASCADE'}},
      {name:'comment',type:'TEXT'},
      {name:'author',type:'VARCHAR(255)'},
      {name:'date',type:'DATETIME'}
    ]})
    
    await Post.rawRun(conn,'SET FOREIGN_KEY_CHECKS = 0;', []);
    await Comment.dropTable(conn);
    await Post.dropTable(conn);
    await Post.createTable(conn);
    await Comment.createTable(conn);
    await Post.rawRun(conn,'SET FOREIGN_KEY_CHECKS = 1;', []);

    console.log("MySQL CRUD");
    await new Post({title:'Post 1',body:'nonononon',author:'Author',date:Post.now()}).save(conn);
    await new Post({title:'Post 2',body:'nonononon',author:'Author',date:Post.now()}).save(conn);
    await new Post({title:'Post 3',body:'nonononon',author:'Author',date:Post.now()}).save(conn);
    await new Comment({postId:1,comment:'comment 1.1',author:'Commenter',date:Post.now()}).save(conn);
    await new Comment({postId:1,comment:'comment 1.2',author:'Commenter',date:Post.now()}).save(conn);
    await new Comment({postId:2,comment:'comment 2.1',author:'Commenter',date:Post.now()}).save(conn);
    await new Comment({postId:2,comment:'comment 2.2',author:'Commenter',date:Post.now()}).save(conn);
    await new Comment({postId:2,comment:'comment 2.3',author:'Commenter',date:Post.now()}).save(conn);
    let post1 = await new Post({id:1}).load(conn);
    post1.body = "new body";
    await post1.save(conn);
    await new Comment({id:2}).delete(conn);

    console.log("MySQL Custom");
    console.log("rawAll");
    console.log(await Post.rawAll(conn,`
      SELECT
        ${Post.getSQLColumns()},
        COUNT(comment.id) as comments
      FROM ${Post.table}
      LEFT JOIN comment ON comment.post_id = post.id
      GROUP BY ${Post.getSQLColumns()}
    `, []));
    console.log("runSelect");
    console.log(await Post.runSelect(conn,`
      SELECT
        ${Post.getSQLColumns()},
        COUNT(comment.id) as comments
      FROM ${Post.table}
      LEFT JOIN comment ON comment.post_id = post.id
      GROUP BY ${Post.getSQLColumns()}
    `, []));

    conn.release();
    pool.end();

    console.log("SQLite Connection");
    let sqliteDB = new sqlite3.Database('blog.sqlite',(err) => {if(err) return console.error(err.message)});

    console.log("SQLite Data Definition");
    Post = Notorm({_dbFlavor:'sqlite',_table:'post',_columns:[
      {name:'id',type:'INTEGER',primaryKey:true,autoIncrement:true},
      {name:'title',type:'TEXT'},
      {name:'body',type:'TEXT'},
      {name:'author',type:'TEXT'},
      {name:'date',type:'TEXT'}
    ]})
    Comment = Notorm({_dbFlavor:'sqlite',_table:'comment',_columns:[
      {name:'id',type:'INTEGER',primaryKey:true,autoIncrement:true},
      {name:'postId',type:'INTEGER'},
      {name:'comment',type:'TEXT'},
      {name:'author',type:'TEXT'},
      {name:'date',type:'TEXT'}
    ]})
    await Post.dropTable(sqliteDB);
    await Post.createTable(sqliteDB);
    await Comment.dropTable(sqliteDB);
    await Comment.createTable(sqliteDB);

    console.log("SQLite CRUD");
    await new Post({title:'Post 1',body:'nonononon',author:'Author',date:Post.now()}).save(sqliteDB);
    await new Post({title:'Post 2',body:'nonononon',author:'Author',date:Post.now()}).save(sqliteDB);
    await new Post({title:'Post 3',body:'nonononon',author:'Author',date:Post.now()}).save(sqliteDB);
    await new Comment({postId:1,comment:'comment 1.1',author:'Commenter',date:Post.now()}).save(sqliteDB);
    await new Comment({postId:1,comment:'comment 1.2',author:'Commenter',date:Post.now()}).save(sqliteDB);
    await new Comment({postId:2,comment:'comment 2.1',author:'Commenter',date:Post.now()}).save(sqliteDB);
    await new Comment({postId:2,comment:'comment 2.2',author:'Commenter',date:Post.now()}).save(sqliteDB);
    await new Comment({postId:2,comment:'comment 2.3',author:'Commenter',date:Post.now()}).save(sqliteDB);
    post1 = await new Post({id:1}).load(sqliteDB);
    post1.body = "new body";
    await post1.save(sqliteDB);
    await new Comment({id:2}).delete(sqliteDB);

    console.log("SQLite Custom");
    console.log("rawAll");
    console.log(await Post.rawAll(sqliteDB,`
      SELECT
        ${Post.getSQLColumns()},
        COUNT(comment.id) as comments
      FROM ${Post.table}
      LEFT JOIN comment ON comment.post_id = post.id
      GROUP BY ${Post.getSQLColumns()}
    `, []));
    console.log("runSelect");
    console.log(await Post.runSelect(sqliteDB,`
      SELECT
        ${Post.getSQLColumns()},
        COUNT(comment.id) as comments
      FROM ${Post.table}
      LEFT JOIN comment ON comment.post_id = post.id
      GROUP BY ${Post.getSQLColumns()}
    `, []));

    sqliteDB.close();

    console.log("Playing with dates");
    // UTC Datetime from timestamp (timestamp is always UTC)
    console.log(Post.timestampToDatetime(Date.now()));
    // Local Datetime from timestamp (timestamp is always UTC)
    console.log(Post.timestampToLocalDatetime(Date.now()));
    // Now local Datetime 
    console.log(Post.now());
    // A date object from mysql will have the local timezone set
    console.log(post1.date);
    console.log(post1.date.toLocaleString());

  }catch(e){
    console.log(e);
  }
};
run();
