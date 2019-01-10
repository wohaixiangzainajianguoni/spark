package second.lambda;

public class java7 {
    public static void main(String[] args) {

//        IEat eat = new eatImpl();
//        eat.eat("apple");

//        IEat iEat = new IEat() {
//            @Override
//            public void eat(String nae) {
//                System.out.println(nae);
//                System.out.println("无实现类");
//            }
//        };
//
//        iEat.eat("sd");

       IEat eat1= (String name)->{
           System.out.println(name);
            System.out.println("拉姆达表达式");
            return 100;
        };

        int names = eat1.eat("names");
        System.out.println(names);


    }
}

interface  IEat{
    public int eat(String fruitName);


}
class  eatImpl  implements  IEat
{
    @Override
    public  int eat(String fruitName) {
        System.out.println("ext"+fruitName);
        return 100;
    }
}

