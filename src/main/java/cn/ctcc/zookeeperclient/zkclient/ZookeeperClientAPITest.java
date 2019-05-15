package cn.ctcc.zookeeperclient.zkclient;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * @Author: zk
 * @Date: 2019/5/9 15:47
 * @Description: Zookeeper原生客户端api----->想查看监听事件，要保证zookeeper的连接没中断,
 * 主方法没结束，监听事件是守护线程。节点监听默认只会触发一次，如果需要永久监听，需要自己创建监听器
 * @Modified:
 * @version: V1.0
 */
public class ZookeeperClientAPITest {


    /**
     * 连接是异步，连接成功会调用Watcher的process发方法，此时连接状态为SyncConnected，事件为：None
     * @throws Exception
     */
    public static void test01() throws Exception{

        //znode节点
        String zNodes="192.168.241.12:2181,192.168.241.13:2181,192.168.241.14:2181";

        //客户端连接服务端是异步的,上面的调用会马上从ZooKeeper构造函数返回，
        //当与服务器建立好连接之后会调用Watcher中的process方法进行处理。
        //process方法会接受一个WatchedEvent类型的参数，用于表明发生了什么事件。
        ZooKeeper zooKeeper = new ZooKeeper(zNodes, 4000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

                //客户端与服务端的连接状态:SyncConnected、Disconnected、ConnectedReadOnly和AuthFailed四种状态
                //SyncConnected
                System.out.println("1----"+watchedEvent.getState());

                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    //连接成功后，watchedEvent.getType()方法获取具体的事件类型。
                    //事件类型的取值包括None、NodeCreated、NodeDeleted、NodeDataChanged和NodeChildrenChanged。
                    //None
                    System.out.println("2----"+watchedEvent.getType());
                }
            }
        });

        //[zookeeper]
        System.out.println("RootChildrens:"+zooKeeper.getChildren("/",false));

        //zooKeeper.close();
    }


    /**
     * 同步方法：创建节点-->PERSISTENT、PERSISTENT_SEQUENTIAL、EPHEMERAL和EPHEMERAL_SEQUENTIAL。
     * @throws Exception
     */
    //@Test
    public  void test02() throws Exception{

        //znode节点
        String zNodes="192.168.241.12:2181,192.168.241.13:2181,192.168.241.14:2181";

        //获取连接
        ZooKeeper zooKeeper = new ZooKeeper(zNodes, 4000, new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    System.out.println("连接成功！");
                    //在这里可以监听事件
                    //Event Type:None
                    System.out.println("Event Type:"+watchedEvent.getType());

                }
            }
        });


        //使用同步方法创建一个节点--->Path must start with / character
        String son1="/node02";
        //同步方法,有返回值，会抛出异常
        //第一个参数是要创建的节点路径。第二个参数是创建节点的数据值，参数类型是字节数组。第三个参数是这个节点的访问权限。第四个参数是创建节点的类型
        String nodePath = zooKeeper.create(son1, "hello word ! -01".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        ///node01
        System.out.println(nodePath);

        //zooKeeper.close();
    }


    /**
     * 异步创建节点
     */
    public static void test03() throws Exception{

        //znode节点
        String zNodes="192.168.241.12:2181,192.168.241.13:2181,192.168.241.14:2181";

        //获取连接
        ZooKeeper zooKeeper = new ZooKeeper(zNodes, 4000, new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    System.out.println("连接成功！");
                    //在这里可以监听事件
                    //Event Type:None
                    System.out.println("Event Type:"+watchedEvent.getType());

                }
            }
        });

       String son2="/node03";

  /*     //异步调用没有返回值，创建是否成功的逻辑在回调中处理
       zooKeeper.create(son2, "你好，世界！".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {

           //该方法会在节点创建好之后被调用。
           //第一个是int类型的resultCode，作为创建节点的结果码，当成功创建节点时，resultCode的值为0。
           //第二个参数是创建节点的路径。
           //第三个参数是context，当一个StringCallback类型对象作为多个create方法的参数时，这个参数就很有用了。
           //第四个参数是创建节点的名字，其实与path参数相同
           @Override
           public void processResult(int i, String s, Object o, String s1) {
               System.out.println("============================================================");
               System.out.println(i);
               System.out.println(s);
               System.out.println(o);
               System.out.println(s1);
           }
       },"context");*/

        zooKeeper.create(son2, "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int resultCode, String path, Object ctx, String name) {
                //0
                System.out.println(resultCode);
                ///node03
                System.out.println(path);
                //创建
                System.out.println(ctx);
                ///node03
                System.out.println(name);
            }
        }, "创建");

        //zooKeeper.close();
    }


    /**
     * 同步获取节点的数据值
     */
    public static void test04() throws Exception{

        //znode节点
        String zNodes="192.168.241.12:2181,192.168.241.13:2181,192.168.241.14:2181";

        //获取连接
         ZooKeeper zooKeeper = new ZooKeeper(zNodes, 4000, new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    System.out.println("连接成功！");
                    //在这里可以监听事件
                    //Event Type:None
                    System.out.println("Event Type:"+watchedEvent.getType());

                    //子节点列表发生变会触发
                    if(watchedEvent.getType()==Event.EventType.NodeChildrenChanged){
                        System.out.println(watchedEvent.getPath());
                    }

                    //节点数据发生变化会触发
                    if(watchedEvent.getType()== Event.EventType.NodeDataChanged){
                        System.out.println(watchedEvent.getPath());
                    }


                }
            }
        });

        //这个会对该节点的子节点监听，但不会进行对该节点及其子节点的数据进行监听
        //zooKeeper.getChildren("/",true,new Stat());

        String son1="/node01";

        Stat stat = new Stat();
        //同步方法获取数据，会触发watcher,stat会被自动填充数据
        //这个只会对当前节点的数据进行监听-->当节点的数据内容发生变化时，我们就会接收到NodeDataChanged这个事件
        byte[] data = zooKeeper.getData(son1, true, stat);

        //aaa
        System.out.println(new String(data));
        //0
        System.out.println(stat.getAversion());
        //3
        System.out.println(stat.getDataLength());

        //zooKeeper.close();
    }

    /**
     * 异步获取节点数值
     */
    public static void test05() throws Exception{

        //znode节点
        String zNodes="192.168.241.12:2181,192.168.241.13:2181,192.168.241.14:2181";

        //获取连接
        ZooKeeper zooKeeper = new ZooKeeper(zNodes, 4000, new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    System.out.println("连接成功！");
                    //在这里可以监听事件
                    //Event Type:None
                    System.out.println("Event Type:"+watchedEvent.getType());

                    //子节点列表发生变会触发
                    if(watchedEvent.getType()==Event.EventType.NodeChildrenChanged){
                        System.out.println(watchedEvent.getPath());
                    }

                    //节点数据发生变化会触发
                    if(watchedEvent.getType()== Event.EventType.NodeDataChanged){
                        System.out.println(watchedEvent.getPath());
                    }


                }
            }
        });

        //当节点的数据内容发生变化时，我们就会接收到NodeDataChanged这个事件
        zooKeeper.getData("/node01", true, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int resultCode, String path, Object context, byte[] bytes, Stat stat) {

                //状态码
                System.out.println(resultCode);
                //节点
                System.out.println(path);
                //context
                System.out.println(context);
                //znode数据
                System.out.println(new String(bytes));
                //znode状态
                System.out.println(stat);
            }
        },"context");



        //zooKeeper.close();

    }


    /**
     * 同步：获取子节点列表
     */
    public static void test06() throws Exception{

        //zone节点列表
        String znodes="192.168.241.12:2181,192.168.241.13:2181,192.168.241.14:2181";

        //获取连接，连接是异步的
        ZooKeeper zooKeeper = new ZooKeeper(znodes, 4000, new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getState()== Event.KeeperState.SyncConnected){

                    System.out.println("已连接服务端....");

                    if(watchedEvent.getType()== Event.EventType.NodeChildrenChanged){
                        System.out.println(watchedEvent.getPath()+":子节点发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDataChanged){
                        System.out.println(watchedEvent.getPath()+":节点数据发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDeleted){
                        System.out.println(watchedEvent.getPath()+":节点被删除~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeCreated){
                        System.out.println(watchedEvent.getPath()+":节点被创建了~");
                    }

                }

            }
        });

        //当节点的子节点列表发生变化时，zk服务器会向我们推送类型为NodeChildrenChanged的事件
        List<String> children = zooKeeper.getChildren("/", true);
        children.forEach(System.out::println);

        //zooKeeper.close();
    }

    /**
     * 异步：获取子节点列表
     * @param
     * @throws Exception
     */
    public static void test07()throws Exception{

        String zNodes="192.168.241.12:2181,192.168.241.13:2181,192.168.241.13:2181";

        ZooKeeper zooKeeper=new ZooKeeper(zNodes, 4000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    System.out.println("已连接服务端....");

                    if(watchedEvent.getType()== Event.EventType.NodeChildrenChanged){
                        System.out.println(watchedEvent.getPath()+":子节点发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDataChanged){
                        System.out.println(watchedEvent.getPath()+":节点数据发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDeleted){
                        System.out.println(watchedEvent.getPath()+":节点被删除~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeCreated){
                        System.out.println(watchedEvent.getPath()+":节点被创建了~");
                    }

                }

            }
        });

        //异步获取子节点列表-->当节点的子节点列表发生变化时，zk服务器会向我们推送类型为NodeChildrenChanged的事件
        zooKeeper.getChildren("/", true, new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(int resCode, String path, Object ctx, List<String> list) {
                //0
                System.out.println(resCode);
                // /
                System.out.println(path);
                //异步获取子节点列表~
                System.out.println(ctx);
                //node01
                //zookeeper
                //node02
                //node03
                list.forEach(System.out::println);
            }
        }, "异步获取子节点列表~");


        //关闭客户端
        zooKeeper.close();
    }


    /**
     * 同步：查看一个节点是否存在
     */
    public static  void test08()throws Exception{

        String zNodes="192.168.241.12:2181,192.168.241.13:2181,192.168.241.13:2181";

        ZooKeeper zooKeeper=new ZooKeeper(zNodes, 4000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    System.out.println("已连接服务端....");

                    if(watchedEvent.getType()== Event.EventType.NodeChildrenChanged){
                        System.out.println(watchedEvent.getPath()+":子节点发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDataChanged){
                        System.out.println(watchedEvent.getPath()+":节点数据发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDeleted){
                        System.out.println(watchedEvent.getPath()+":节点被删除~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeCreated){
                        System.out.println(watchedEvent.getPath()+":节点被创建了~");
                    }

                }

            }
        });

        //exists方法的watch参数比较特别，如果将其指定为true，那么代表你对该节点的创建事件、节点删除事件和该节点的数据内容改变事件都感兴趣
        //虽然监听三个事件，但是事件只会触发一次，因此谁先触发，谁先打印
        Stat stat = zooKeeper.exists("/node10", true);
        //如果不存在则返回null
        System.out.println(stat);

        //zooKeeper.close();
    }


    /**
     * 异步查看一个节点是否存在
     */
    public static void test09()throws  Exception{

        String zNodes="192.168.241.12:2181,192.168.241.13:2181,192.168.241.13:2181";

        ZooKeeper zooKeeper=new ZooKeeper(zNodes, 4000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    System.out.println("已连接服务端....");

                    if(watchedEvent.getType()== Event.EventType.NodeChildrenChanged){
                        System.out.println(watchedEvent.getPath()+":子节点发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDataChanged){
                        System.out.println(watchedEvent.getPath()+":节点数据发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDeleted){
                        System.out.println(watchedEvent.getPath()+":节点被删除~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeCreated){
                        System.out.println(watchedEvent.getPath()+":节点被创建了~");
                    }

                }

            }
        });

        //会监听，节点被删除，节点创建，节点数据发生变化的事件
        zooKeeper.exists("/node11", true, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int resCode, String path, Object ctx, Stat stat) {
                //非0状态吗为不正常
                System.out.println(resCode);
                System.out.println(path);
                System.out.println(ctx);
                //如果节点不存在，stat为空
                System.out.println(stat.getAversion());

            }
        },"异步查看节点是否存在~");


        //zooKeeper.close();
    }


    /**
     *
     *  同步:修改节点数据
     */
    public static void test10() throws Exception{

        String zNodes="192.168.241.12:2181,192.168.241.13:2181,192.168.241.13:2181";

        ZooKeeper zooKeeper=new ZooKeeper(zNodes, 4000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    System.out.println("已连接服务端....");

                    if(watchedEvent.getType()== Event.EventType.NodeChildrenChanged){
                        System.out.println(watchedEvent.getPath()+":子节点发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDataChanged){
                        System.out.println(watchedEvent.getPath()+":节点数据发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDeleted){
                        System.out.println(watchedEvent.getPath()+":节点被删除~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeCreated){
                        System.out.println(watchedEvent.getPath()+":节点被创建了~");
                    }

                }

            }
        });

        Stat st = new Stat();
        byte[] bytes = zooKeeper.getData("/node01", true, st);
        System.out.println(new String(bytes));

        //只有当version参数的值与节点状态信息中的dataVersion值相等时，数据修改才能成功，否则会抛出BadVersion异常
        //为了防止丢失数据的更新，在ZooKeeper提供的API中，所有的写操作(例如后面要提到的delete)都有version参数。
        //version=-1代表最新版本
        Stat stat = zooKeeper.setData("/node01", "node01经常用".getBytes(), -1);
        System.out.println(stat);

        //zooKeeper.close();
    }


    /**
     * 异步:修改数据
     * @param
     * @throws Exception
     */
    public static void test11()throws Exception{

        String zNodes="192.168.241.12:2181,192.168.241.13:2181,192.168.241.13:2181";

        ZooKeeper zooKeeper=new ZooKeeper(zNodes, 4000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    System.out.println("已连接服务端....");

                    if(watchedEvent.getType()== Event.EventType.NodeChildrenChanged){
                        System.out.println(watchedEvent.getPath()+":子节点发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDataChanged){
                        System.out.println(watchedEvent.getPath()+":节点数据发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDeleted){
                        System.out.println(watchedEvent.getPath()+":节点被删除~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeCreated){
                        System.out.println(watchedEvent.getPath()+":节点被创建了~");
                    }

                }

            }
        });


        Stat st = new Stat();
        byte[] bytes = zooKeeper.getData("/node01", true, st);
        System.out.println(new String(bytes));

        zooKeeper.setData("/node01", "天下第一".getBytes(), -1, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int resCode, String path, Object ctx, Stat stat) {
                //0
                System.out.println(resCode);
                // /node01
                System.out.println(path);
                //异步修改节点数据~
                System.out.println(ctx);
                //0
                System.out.println(stat.getAversion());
            }
        },"异步修改节点数据~");

        //zooKeeper.close();

    }

    /**
     * 同步：删除一个节点
     * @throws Exception
     */
    public static void test12()throws Exception{

        String zNodes="192.168.241.12:2181,192.168.241.13:2181,192.168.241.13:2181";

        ZooKeeper zooKeeper=new ZooKeeper(zNodes, 4000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    System.out.println("已连接服务端....");

                    if(watchedEvent.getType()== Event.EventType.NodeChildrenChanged){
                        System.out.println(watchedEvent.getPath()+":子节点发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDataChanged){
                        System.out.println(watchedEvent.getPath()+":节点数据发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDeleted){
                        System.out.println(watchedEvent.getPath()+":节点被删除~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeCreated){
                        System.out.println(watchedEvent.getPath()+":节点被创建了~");
                    }

                }

            }
        });

        zooKeeper.exists("/node11",true);
        zooKeeper.delete("/node11",-1);

        //zooKeeper.close();
    }

    /**
     * 异步：删除一个节点
     */
    public static void test13()throws Exception{
        String zNodes="192.168.241.12:2181,192.168.241.13:2181,192.168.241.13:2181";

        ZooKeeper zooKeeper=new ZooKeeper(zNodes, 4000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    System.out.println("已连接服务端....");

                    if(watchedEvent.getType()== Event.EventType.NodeChildrenChanged){
                        System.out.println(watchedEvent.getPath()+":子节点发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDataChanged){
                        System.out.println(watchedEvent.getPath()+":节点数据发生了变化~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeDeleted){
                        System.out.println(watchedEvent.getPath()+":节点被删除~");
                    }

                    if(watchedEvent.getType()== Event.EventType.NodeCreated){
                        System.out.println(watchedEvent.getPath()+":节点被创建了~");
                    }

                }

            }
        });


        zooKeeper.exists("/node10",true);
        zooKeeper.delete("/node10", -1, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int resCode, String path, Object ctx) {
                //0
                System.out.println(resCode);
                // /node10
                System.out.println(path);
                //异步删除一个节点~
                System.out.println(ctx);
            }
        },"异步删除一个节点~");

        //zooKeeper.close();
    }



    public static void main(String[] args) throws Exception {

       //test01();
       //test02();
       //test03();

       //test04();

       //test05();
       //test06();
       //test07();
       //test08();
       //test09();
       //test10();
       //test11();
       // test12();
        test13();
       //保证主线程不退出
       System.in.read();
    }





}
