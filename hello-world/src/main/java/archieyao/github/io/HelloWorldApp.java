package archieyao.github.io;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/** @author ArchieYao Created: 2022/1/4 12:49 下午 Description: */
public class HelloWorldApp {
    public static void main(String[] args) {
        BitSet bitSet = new BitSet();
        bitSet.set(64, true);
        bitSet.set(65, true);
        bitSet.set(6, true);
        bitSet.set(128, true);
        bitSet.set(2048, true);
        bitSet.set(3049, true);
        bitSet.set(4096, true);
        System.out.println(Arrays.toString(bitSet.toLongArray()));

        System.out.println(2 >> 6);

        //        System.out.println("hello world");
        //
        //        int hashCode = Objects.hashCode("359533051729330");
        //        System.out.println(hashCode);
        //        System.out.println(hashCode % 3);
        //
        //        System.out.println((-2 & 2147483647));
        //        System.out.println((hashCode & Integer.MAX_VALUE) % 3);
        //
        //        ArrayList<String> objects = new ArrayList<>();
        //        objects.add("1111");
        //        Stu stu = new Stu();
        //        stu.name="sss";
        //        stu.score = objects;
        //
        //        System.out.println(stu);
        //
        //        List<String> score = stu.getScore();
        //        score = new ArrayList<>();
        //        score.add("2222");
        //        System.out.println(stu);
    }

    private static class Stu {
        private String name;
        private List<String> score;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<String> getScore() {
            return score;
        }

        public void setScore(List<String> score) {
            this.score = score;
        }

        @Override
        public String toString() {
            return "Stu{" + "name='" + name + '\'' + ", score=" + score + '}';
        }
    }
}
