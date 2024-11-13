package org.example;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class StreamProcessingExample {
    // Define a fixed-size queue to handle backpressure
    private static final int QUEUE_CAPACITY = 10;
    private static final BlockingQueue<String> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

    public static void main(String[] args) {
        // Start producer and consumer threads
        Thread producer = new Thread(new DataProducer());
        Thread consumer = new Thread(new DataConsumer());

        producer.start();
        consumer.start();
    }

    // Producer simulates incoming stream data
    static class DataProducer implements Runnable {
        @Override
        public void run() {
            try {
                for (int i = 1; i <= 1000; i++) { // Simulating 100 data items
                    String data = "Data " + i;
                    // Put data into the queue (blocking if full, to apply backpressure)
                    queue.put(data);
                    System.out.println("Produced: " + data);
                    // Simulate incoming data rate
                    Thread.sleep(50);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Producer interrupted");
            }
        }
    }

    // Consumer processes data from the queue
    static class DataConsumer implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    // Take data from the queue and process it
                    String data = queue.take();
                    System.out.println("Consumed: " + data);
                    // Simulate processing time
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Consumer interrupted");
            }
        }
    }
}