import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/2 16:07 PM
 */
public class Test {
    public static void main(String[] args) {

        List<Long> ids = new ArrayList<>(400000);

        for (int i = 0; i < 400000; i++) {
            System.out.println(i);
            ids.add((long) new Random().nextInt(8000000));
        }

        ids.forEach(s-> System.out.println(s));
        System.out.println(ids.size());

    }
}
