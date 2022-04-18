package archieyao.github.io;

import java.util.Objects;

/**
 * @author ArchieYao
 * Created: 2022/1/4 12:49 下午
 * Description:
 */
public class HelloWorldApp {
    public static void main(String[] args) {
        System.out.println("hello world");

        int hashCode = Objects.hashCode("359533051729330");
        System.out.println(hashCode);
        System.out.println(hashCode % 3);

        System.out.println((-2 & 2147483647));
        System.out.println((hashCode & Integer.MAX_VALUE) % 3);

    }
}
