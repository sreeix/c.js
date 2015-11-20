;; This buffer is for notes you don't want to save, and for Lisp evaluation.
;; If you want to create a file, visit that file with C-x C-f,
;; then enter the text in that file's own buffer.

var zookeeper = require("node-zookeeper-client");

var client = zookeeper.createClient('localhost:2181');

    var testRoot = "/test-temp";

        client.connect();
        client.once('connected', function () {
            console.log("Connected to zookeeper");
        });

var logme = function (err, path) {console.log(arguments);}
client.create(testRoot, new Buffer("test"), zookeeper.CreateMode.PERSISTENT,logme)



var nodeDataWatcher = function(event) {
    console.log("Watch", arguments);
    if(event.name !== 'NODE_DELETED') {
        getAndWatchNodeData(event.path);
    }
}
function getAndWatchNodeData(path) {
    client.getData(path, nodeDataWatcher, function(err, data){console.log("Got Data", data.toString('utf-8'));})
}
getAndWatchNodeData(testRoot);


client.mkdirp(testRoot+"/foo/bar", zookeeper.CreateMode.PERSISTENT,logme);
client.remove(testRoot+"/foo/bar",logme);
client.remove(testRoot+"/foo",logme);

client.mkdirp(testRoot+"/foo", zookeeper.CreateMode.PERSISTENT,logme);


client.remove(testRoot,logme);

var childrenwatch = function (event) {

    console.log("Children Watch", event);
    if(event.name !== 'NODE_DELETED') {
        getAndWatchNodeChildren(event.path);
    }
}
function getAndWatchNodeChildren(path) {
    client.getChildren(path, childrenwatch, function  (err, children, stat) {
        console.log(children, stat);
    });

}
getAndWatchNodeChildren(testRoot);


client.setData(testRoot, new Buffer('Tests1'), logme);

//Watchers for data apply only once

// A node data changes can be watched forever by recursive calls to get data.
// data watch is not triggered for child creation (recursive)
// data watch is not triggere for child removal(recursive)


// get children fn returns all children and stat for the node. (does it have ephemeral vs persistent)
// child watcher gets child changed on delete and add

// looks like adding a watch on the same node from same session does not increase the watch count. But invocation causes multiple notifications. SO bad idea to indiscriminately add ones(probably a bug in node-zk). Since zk only keeps one watch they are all invoked simultanously and then any other change there is no watch
// Deletion of the node will trigger self watch as well as child watch.

// connection loss happens when close on the connection is called when there are events on the pipe


//Read up on the exist watcher(looks like it will do a created/deleted events)

client.remove(testRoot,logme);







// .each(function (child) {
//                 x.children.push(child);
//                 console.log("---- Got all children. Now adding data watch on it", zkPath.join(path, child));
//                 return l.addSelfAndChildWatcher(zkPath.join(path, child), options, onWatch);
//             })




// function () {
//                return client.getChildrenAsync(path, function childrenWatcher(event) {
//                    console.log("yeah got the watch", event);
//                    // register the next watch iff it was not deleted.
//                    if(event.name === 'NODE_DELETED') {
//                        console.log("node with " + event.path + " has been deleted removing from teh list");
//                        x.children = _.reject(x.children, function (c) {
//                            return x.path === event.path;
//                        });
//                        return onWatch(event);
//                    } if(event.name === 'NODE_CHILDREN_CHANGED') {
//                        // client.getChildren(path, myWatcher, function  (err, data) {
//                        //     if(err){
//                        //         console.log("Error happened getting the data from zookeeper for path "+ path, err);
//                        //         return;
//                         //     }
//                         //     return onWatch(event);
//                         // });
//                         l.watchAllChildren(path, options, onWatch).then(function (children) {
//                             console.log("Registered watch on the path", path);
//                             return onWatch(event);
//                         });
//                     }
//                 }).get(0).each(function (child) {
//                     return l.addSelfAndChildWatcher(zkPath.join(path, child), options, onWatch).then(function (ch) {
//                         x.children.push(ch);
//                     })
//                 }).return(x).tap(console.log);
//             }
