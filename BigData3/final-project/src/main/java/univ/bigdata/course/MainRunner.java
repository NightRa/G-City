package univ.bigdata.course;

public class MainRunner {

    public static void main(String[] args) {
        args = new String[2];
        args[0] = "pagerank";
        args[1] = "C:\\Users\\user\\BigData\\G-City\\BigData3\\BigDataset\\movies-simple4.txt";
        SparkMain.main(args);
    }
}
