import com.mysql.jdbc.Driver;

import java.util.Arrays;
import java.util.List;

public class test {
    public static void main(String[] args) {
//        System.out.println(Driver.class.getName());
        List<Integer> list = Arrays.asList(1);
        list.sort((o1, o2) -> o2-o1);

        System.out.println(list.toString());
    }
}
