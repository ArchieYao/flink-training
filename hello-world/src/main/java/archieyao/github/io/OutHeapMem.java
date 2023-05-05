package archieyao.github.io;

import sun.nio.ch.DirectBuffer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class OutHeapMem {
    public static void main(String[] args) throws Exception {
//        TimeUnit.SECONDS.sleep(60);
        System.out.println("===================");
        ByteBuffer bb = ByteBuffer.allocateDirect(1024 * 1024 * 1024);
        ByteBuffer buffer = bb.put(new byte[]{1});
        ByteBuffer buffer1 = bb.get(buffer.array());
        System.out.println(buffer1.toString());
        TimeUnit.SECONDS.sleep(30);

        System.out.println("===================");
        // 清除直接缓存
        ((DirectBuffer) bb).cleaner().clean();
        System.out.println("ok");

        TimeUnit.SECONDS.sleep(30);

    }

    private static void readFile1() throws IOException {
        File file = new File("D://tmp/a.txt");
        try (FileReader fileReader = new FileReader(file)) {
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            int count;
            StringBuilder stringBuilder = new StringBuilder();
            while ((count = bufferedReader.read()) != -1) {
                // 还可以按行读取bufferedReader.readLine();
                stringBuilder.append((char) count);
            }
            System.out.println(stringBuilder.toString());
        }
    }
}
