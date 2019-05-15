package cn.ctcc.zookeeperclient.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @Author: zk
 * @Date: 2019/5/13 16:39
 * @Description:
 * Apache Curator是一个比较完善的ZooKeeper客户端框架，通过封装的一套高级API 简化了ZooKeeper的操作。通过查看官方文档，可以发现Curator主要解决了三类问题：
 *
 *      封装ZooKeeper client与ZooKeeper server之间的连接处理
 *      提供了一套Fluent风格的操作API
 *      提供ZooKeeper各种应用场景(recipe， 比如：分布式锁服务、集群领导选举、共享计数器、缓存机制、分布式队列等)的抽象封装
 *
 * Curator主要从以下几个方面降低了zk使用的复杂性：
 *
 *     重试机制:提供可插拔的重试机制, 它将给捕获所有可恢复的异常配置一个重试策略，并且内部也提供了几种标准的重试策略(比如指数补偿)
 *     连接状态监控: Curator初始化之后会一直对zk连接进行监听，一旦发现连接状态发生变化将会作出相应的处理
 *     zk客户端实例管理:Curator会对zk客户端到server集群的连接进行管理，并在需要的时候重建zk实例，保证与zk集群连接的可靠性
 *     各种使用场景支持:Curator实现了zk支持的大部分使用场景（甚至包括zk自身不支持的场景），这些实现都遵循了zk的最佳实践，并考虑了各种极端情况
 * @Modified:
 * @version: V1.0
 */
public class CuratorAPITest {


    /**
     * 会话超时时间
     */
    private final int SESSION_TIMEOUT=30*1000;

    /**
     * 连接超时时间
     */
    private final int CONNECTION_TIMEOUT=3*1000;

    /**
     * znode节点
     */
    private static final String SERVER="192.168.241.12:2181,192.168.241.13:2181,192.168.241.14:2181";


    /**
     * 创建客户端实例
     */
    private CuratorFramework curatorFramework=null;


    /**
     * baseSleepTimeMs：初始的重试等待时间
     * maxRetries：最多重试次数
     *Curator默认提供了以下几种重试策略:
     *     ExponentialBackoffRetry：重试一定次数，每次重试时间依次递增
     *     RetryNTimes：重试N次
     *     RetryOneTime：重试一次
     *     RetryUntilElapsed：重试一定时间
     *
     */
    ExponentialBackoffRetry retryPolicy= new ExponentialBackoffRetry(1000,3);


    @Before
    public void init(){

        //创建客户端实例
        curatorFramework= CuratorFrameworkFactory.newClient(SERVER,SESSION_TIMEOUT,CONNECTION_TIMEOUT,retryPolicy);

        //启动
        curatorFramework.start();
    }


    /**
     * 测试创建节点
     *
     */
    @Test
    public void test01()throws Exception{


        //创建永久节点
        String path01 = curatorFramework.create().forPath("/curator", "curator hello word".getBytes());

        //创建永久有序节点
        String path02 = curatorFramework.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/curator_sql", "curator_sql".getBytes());

        //创建临时有序节点
        String path03 = curatorFramework.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath("/curator/ephemeral_path1", "/curator/ephemeral_path1 data".getBytes());

        //在创建临时有序节点的时候可以使用withProtection() 方法，该方法的作用是在创建的节点名前面添加GUID标识，
        //其目的是为了避免出现“节点创建成功，但是ZooKeeper服务器在创建的节点名被返回给client前就出现了异常，
        //从而导致临时节点没有被立即删除,而客户端认为创建失败
        String path04 = curatorFramework.create().withProtection().withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                .forPath("/curator/ephemeral_path2", "/curator/ephemeral_path2 data".getBytes());

        // /curator
        System.out.println(path01);
        // /curator_sql0000000013
        System.out.println(path02);
        // /curator/ephemeral_path10000000000
        System.out.println(path03);
        // /curator/_c_8a8fb8bb-36fb-4c66-9522-1164c8bb9570-ephemeral_path20000000001
        System.out.println(path04);
    }


    /**
     * 检测某节点是否存在
     * @throws Exception
     */
    @Test
    public void test02()throws Exception{

        Stat stat01 = curatorFramework.checkExists().forPath("/node01");
        Stat stat02 = curatorFramework.checkExists().forPath("/node02");

        // /node01:不存在!
        System.out.println("/node01:"+(stat01==null?"不存在!":"存在!"));
        // /node02:存在!
        System.out.println("/node02:"+(stat02==null?"不存在!":"存在!"));
    }

    /**
     * 获取和设置节点数据
     */
    @Test
    public void test03() throws Exception{

        //获取指定节点的子节点
        List<String> paths = curatorFramework.getChildren().forPath("/");
        //curator
        //zookeeper
        //node02
        //node03
        //curator_sql0000000013
        paths.forEach(System.out::println);

        //获取指定节点的数据
        byte[] pathData = curatorFramework.getData().forPath("/"+paths.get(0));
        //curator hello word
        System.out.println(new String(pathData));


        //设置指定节点的数据
        Stat stat = curatorFramework.setData().forPath("/node02", "小宝贝~~~".getBytes());
        //42949672971,51539607558,1557726800295,1557798152173,1,0,0,0,12,0,42949672971
        System.out.println(stat);

    }


    /**
     * 异步设置节点数据(其它异步操作也支持)以及获取通知(设置异步通知的全局监听器)
     */
    @Test
    public void test04()throws Exception{

        //添加监听器--->推测出该监听器是全局监听器，用来监听异步操作，同步操作不会被通知
        curatorFramework.getCuratorListenable().addListener(new CuratorListener() {
            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {

                System.out.println(event.getType().name());
                System.out.println(event.getPath());
                //异步设置节点数据成功后也没不到数据
                byte[] data = event.getData();
                if(data!=null&&data.length>0){
                    System.out.println(new String (data));
                }
            }
        });

        //异步设置节点数据--->这句执行完毕后，先打印WATCHED，null，再打印SET_DATA，/node03
        curatorFramework.setData().inBackground().forPath("/node03","我来啦~~".getBytes());
        //会打印：CREATE   /node01
        curatorFramework.create().withMode(CreateMode.PERSISTENT).inBackground().forPath("/node01","node01".getBytes());
        //没打印数据->监听器没执行
        curatorFramework.delete().forPath("/node01");
        //防止程序结束，而看不到结果
        System.in.read();
    }

    /**
     *异步操作并获取通知(设置回调的方式--->针对某一znode)
     */
    @Test
    public void test05()throws Exception{

        //异步获取节点数据
        curatorFramework.getData().inBackground(new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                //GET_DATA
                System.out.println(event.getType());
                ///node02
                System.out.println(event.getPath());
                byte[] data = event.getData();
                if(data!=null&&data.length>0){
                    //可以获取到节点数据->小宝贝~~~
                    System.out.println(new String (data));
                }
            }
        }).forPath("/node02");

        System.in.read();
    }


    /**
     *删除节点
     */
    @Test
    public void test06()throws Exception{

        //orSetData():如果存在就设置新值，否则创建并设置新值  creatingParentContainersIfNeeded:级联创建父节点
        curatorFramework.create().orSetData().creatingParentContainersIfNeeded().forPath("/001/002/003","003".getBytes());

        //节点不存在，删除会报错
        curatorFramework.delete().forPath("/node03");

        //删除001，级联删除子节点   guaranteed()---->如果服务端可能删除成功，但是client没有接收到删除成功的提示，Curator将会在后台持续尝试删除该节点
        curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath("/001");
    }


    /**
     * 测试事务管理：碰到异常，事务会回滚
     * @throws Exception
     */
    @Test
    public void test07()throws Exception{

        //定义几个基本的操作
        CuratorOp curatorOp01 = curatorFramework.transactionOp().create().forPath("/node02/aa", "小丑开始表演！".getBytes());

        CuratorOp curatorOp02 = curatorFramework.transactionOp().setData().forPath("/node02", "我有好多分身~".getBytes());

        //这句话会报错，因为有子节点-->KeeperErrorCode = Directory not empty
        CuratorOp curatorOp03 = curatorFramework.transactionOp().delete().forPath("/node02");

        //执行一系列事务操作，并得到事务的执行结果：其中有一项报错，全部操作都失败
        List<CuratorTransactionResult> curatorTransactionResults = curatorFramework.transaction().forOperations(curatorOp01, curatorOp02, curatorOp03);

        curatorTransactionResults.forEach(e->{
            System.out.println(e.getForPath());
            System.out.println(e.getResultStat());
            System.out.println(e.getType());
            System.out.println("--------------------------");
        });

    }


    /**
     * 测试命名空间
     */
    @Test
    public void test08()throws Exception{

        //创建带命名空间的连接实例-->namespace("zk/dev"),不是/开头
        CuratorFramework curator = CuratorFrameworkFactory.builder().namespace("zk/dev")
                .connectString(SERVER)
                .connectionTimeoutMs(CONNECTION_TIMEOUT)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(retryPolicy)
                .build();

        //启动
        curator.start();

        curator.create()
               .orSetData()
               .creatingParentContainersIfNeeded()
               .forPath("/modul01/name01","demo".getBytes());

        curator.close();
    }



    @After
    public void close(){

        curatorFramework.close();
    }


/***************************************************使用Apache Curator监听Zookeeper的某个节点或者目录**************************************************/

    /**
     * 官方推荐的API提供了三个接口，分别如下：
     *
     *     NodeCache
     *
     * 对一个节点进行监听，监听事件包括指定路径的增删改操作
     *
     *     PathChildrenCache
     *
     * 对指定路径节点的一级子目录监听，不对该节点的操作监听，对其子目录的增删改操作监听
     *
     *     TreeCache
     *
     * 综合NodeCache和PathChildrenCahce的特性，是对整个目录进行监听，可以设置监听深度。
     */


    /**
     * 节点路径不存在时，set不触发监听
     * 节点路径不存在，创建事件触发监听
     * 节点路径存在，set触发监听
     * 节点路径存在，delete触发监听
     * 节点挂掉，未触发任何监听
     * 节点重连，未触发任何监听
     * 节点重连 ，恢复监听
     * 对一个节点进行监听，监听事件包括指定路径的创建，删除，修改数据操作，默认就是重复监听
     */
    @Test
    public void test09()throws Exception{

        //1. 创建一个NodeCache
        NodeCache nodeCache = new NodeCache(curatorFramework, "/curator/test_nodeCache");

        //2. 添加节点监听器
        nodeCache.getListenable().addListener(()->{
            ChildData childData = nodeCache.getCurrentData();
            System.out.println("=============================开头=================");
            //以下都可能空指针异常
            System.out.println(new String(childData.getData()));
            System.out.println(childData.getPath());
            System.out.println(childData.getStat());
            System.out.println("===========================结尾===================");
        });

        //3. 启动监听器:如果为true则首次不会缓存节点内容到cache中，默认为false,设置为true首次不会触发监听事件
        //rue:首次启动会加载zookeeper所有节点到缓存中，但不触发监听器
        //false:首次启动会加载zookeeper所有节点到缓存中，但会触发监听器
        nodeCache.start(true);

        //节点不存在，set（不触发监听,抛异常:NoNode for /curator/test_nodeCach）
        //curatorFramework.setData().forPath("/curator/test_nodeCache","nodeCache_value_1".getBytes());
        //节点不存在，create（触发监听）
        //curatorFramework.create().forPath("/curator/test_nodeCache","nodeCache_value_2".getBytes());
        //节点存在，set（触发监听）
        //curatorFramework.setData().forPath("/curator/test_nodeCache","nodeCache_value_3".getBytes());
        //节点存在，delete（触发监听）
        //curatorFramework.delete().forPath("/curator/test_nodeCache");


        System.in.read();
        //nodeCache.close();
    }


    /**
     * 对指定路径节点的一级子目录监听，不对该节点的操作监听，对其子目录的增删改操作监听
     */
    @Test
    public void test10()throws Exception{

        PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework, "/", true);
        pathChildrenCache.getListenable().addListener((curator,event)->{
            ChildData childData = event.getData();
            if(childData != null){
                System.out.println("=============================开头=================");
                System.out.println("Path: " + childData.getPath());
                System.out.println("Stat:" + childData.getStat());
                System.out.println("Data: "+ new String(childData.getData()));
                System.out.println("=============================开头=================");
            }
            switch (event.getType()){
                case CHILD_ADDED:
                    System.out.println("正在新增子节点：" + childData.getPath());
                    //获取子节点
                    List<String> list = curatorFramework.getChildren().forPath("/curator");
                    list.forEach(System.out::println);
                    break;
                case CHILD_UPDATED:
                    System.out.println("正在更新子节点："  + childData.getPath());
                    break;
                case CHILD_REMOVED:
                    System.out.println("子节点被删除");
                    break;
                case CONNECTION_LOST:
                    System.out.println("连接丢失");
                    break;
                case CONNECTION_SUSPENDED:
                    System.out.println("连接被挂起");
                    break;
                case CONNECTION_RECONNECTED:
                    System.out.println("恢复连接");
                    break;
            }
        });


        /**
         * 启动时的模型：PathChildrenCache.StartMode说明如下
         *      POST_INITIALIZED_EVENT:该模型会触发以下事件
         *          1、在监听器启动的时候即，会枚举当前路径所有子节点，触发CHILD_ADDED类型的事件
         *          2、同时会监听一个INITIALIZED类型事件
         *      NORMAL(默认):该模型会触发以下事件
         *                  在监听器启动的时候即，会枚举当前路径所有子节点，触发CHILD_ADDED类型的事件
         *      BUILD_INITIAL_CACHE: 不会触发上面两者事件,同步初始化客户端的cache，及创建cache后，就从服务器端拉入对应的数据
         */
        //开启监听器
        //pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        //子节点不存在，set（不触发监听）
        //curatorFramework.setData().forPath("/test_pathChildrenCache","pathChildrenCache_value_1".getBytes());
        //子节点不存在，create（触发监听）
        //curatorFramework.create().forPath("/test_pathChildrenCache","pathChildrenCache_value_2".getBytes());

        //子节点存在，set（触发监听）
        //curatorFramework.setData().forPath("/test_pathChildrenCache","pathChildrenCache_value_3".getBytes());
        //子节点存在，create子节点的子节点（不触发监听）
        //curatorFramework.create().forPath("/test_pathChildrenCache","child_value_4".getBytes());
        //子节点存在，delete（触发监听）
        //curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath("/test_pathChildrenCache");


        System.in.read();
        //关闭PathChildrenCache
        pathChildrenCache.close();

    }

    /**
     *TreeCache.nodeState == LIVE的时候，才能执行getCurrentChildren非空,默认为PENDING,初始化完成之后，监听节点操作时 TreeCache.nodeState == LIVE
     *
     * maxDepth值设置说明，比如当前监听节点/t1，目录最深为/t1/t2/t3/t4,则maxDepth=3,说明下面3级子目录全 监听，即监听到t4，如果为2，则监听到t3,对t3的子节点操作不再触发, maxDepth最大值2147483647
     *  [type=NODE_UPDATED],set更新节点值操作，范围[当前节点，maxDepth目录节点](闭区间)
     * [type=NODE_ADDED] 增加节点 范围[当前节点，maxDepth目录节点](左闭右闭区间)
     * [type=NODE_REMOVED] 删除节点， 范围[当前节点， maxDepth目录节点](闭区间),删除当前节点无异常
     * @throws Exception
     */
    @Test
    public void test11()throws Exception{

        //创建TreeCache
        TreeCache treeCache = TreeCache.newBuilder(curatorFramework, "/")
                .setCacheData(true)
                .setMaxDepth(5)
                .build();

        //添加监听
        treeCache.getListenable().addListener((curator,event)->{
            System.out.println("==============start============");
            ChildData childData = event.getData();
            if(childData != null){
                System.out.println("Path: " + childData.getPath());
                System.out.println("Stat:" + childData.getStat());
                System.out.println("Data: "+ new String(childData.getData()));
            }

            switch (event.getType()){
                case NODE_ADDED:
                    System.out.println("正在新增子节点：" + childData.getPath());
                    break;
                case NODE_UPDATED:
                    System.out.println("正在更新节点："  + childData.getPath());
                    break;
                case NODE_REMOVED:
                    System.out.println("节点被删除：" + childData.getPath());
                    break;
                case CONNECTION_LOST:
                    System.out.println("连接丢失");
                    break;
                case CONNECTION_SUSPENDED:
                    System.out.println("连接被挂起");
                    break;
                case CONNECTION_RECONNECTED:
                    System.out.println("恢复连接");
                    break;
                case INITIALIZED:
                    System.out.println("初始化连接，并缓存节点数据完毕！");
                    break;
            }

            System.out.println("==============end============");

        });


        //开启监听---》TreeCache只有这个start,默认情况下监听器会枚举所有节点，然后缓存，会先触发NODE_ADDED，然后触发INITIALIZED事件
        treeCache.start();


        //子节点不存在，set（不触发监听）
        //curatorFramework.setData().forPath("/curator/test_pathChildrenCache","pathChildrenCache_value_1".getBytes());
        //子节点不存在，create（触发监听）
        //curatorFramework.create().forPath("/curator/test_pathChildrenCache","pathChildrenCache_value_2".getBytes());

        //子节点存在，set（触发监听）
        //curatorFramework.setData().forPath("/curator/test_pathChildrenCache","pathChildrenCache_value_3".getBytes());
        //子节点存在，create子节点的子节点（触发监听）
        //curatorFramework.create().forPath("/curator/test_pathChildrenCache/aaaa","child_value_4".getBytes());
        //子节点存在，delete（触发监听）
        //curatorFramework.delete().guaranteed().forPath("/curator/test_pathChildrenCache");

        //set监听的根节点（触发监听）
        //curatorFramework.setData().forPath("/","curator_value_aaa".getBytes());

        //delete监听的根节点（触发监听）
        //curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath("/");


        System.in.read();
        //关闭TreeCache
        treeCache.close();


    }


    /**
     * Curator支持原生的Watcher,但是该监听器不再是重复监听，即监听只生效一次
     */
    @Test
    public void test12()throws Exception{

        //自定义一个原生的监听器
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("连接已完成------------");
                    Event.EventType watchedEventType = watchedEvent.getType();
                    if (watchedEventType != null) {
                        System.out.println(watchedEventType);
                    }
                    if (watchedEventType == Event.EventType.NodeChildrenChanged) {
                        System.out.println("该节点的子节点列表已发生变化~~~~");
                        try {
                            //重复监听
                            curatorFramework.getChildren().usingWatcher(this).forPath(watchedEvent.getPath());
                        } catch (Exception e) {
                            throw new RuntimeException(e.getMessage());
                        }
                    }
                }
            }
        };


        curatorFramework.getChildren().usingWatcher(watcher).forPath("/001");

        System.in.read();
    }











}
