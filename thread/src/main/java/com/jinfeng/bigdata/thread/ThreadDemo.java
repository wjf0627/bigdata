package com.jinfeng.bigdata.thread;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @package: com.jinfeng.bigdata.thread
 * @author: wangjf
 * @date: 2019-08-30
 * @time: 17:31
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
public class ThreadDemo {
    static List<String> list = Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) throws InterruptedException {
        final AtomicInteger flag = new AtomicInteger(0);

        list.add("a");
        list.add("b");
        list.add("c");
        list.add("d");

        /*
        new Thread() {
            public void run() {
                Iterator<String> iterator = list.iterator();

                while (iterator.hasNext()) {
                    System.out.println(Thread.currentThread().getName()
                            + ":" + iterator.next());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

            ;
        }.start();

        new Thread() {
            public synchronized void run() {
                Iterator<String> iterator = list.iterator();

                while (iterator.hasNext()) {
                    String element = iterator.next();
                    System.out.println(Thread.currentThread().getName()
                            + ":" + element);
                    if (element.equals("c")) {
                        list.remove(element);
                    }
                }
            }

            ;
        }.start();
         */

        for (int c = 0; c < 100; c++) {
            ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                    .setNameFormat("baichuan-%d").build();
            ExecutorService pool = new ThreadPoolExecutor(100, 200,
                    120L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
            for (int t = 0; t < 50; t++) {
                //  int finalAppId1 = 1;
                //  优先处理 IOS 设备
                //  int finalAppOs = 2;
                /**
                 * process tmail
                 */

                int finalC = c;
                int finalT = t;
                pool.execute(() -> {
                    int index = flag.incrementAndGet();
                    if(index % 1000 == 0){
                        System.out.println(list.size());
                        list.add(String.valueOf(finalC * finalT));
                    }else{
                        list.add(String.valueOf(finalC * finalT));
                    }
                });
            }
            pool.shutdown();
            Thread.sleep(200);
        }
    }
}
