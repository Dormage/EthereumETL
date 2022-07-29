import java.util.List;

public class Interpreter implements Runnable{
    List<String[]> parsedRows;
    Config config;
    Status status;

    public Interpreter(List<String[]> parsedRows, Config config, Status status) {
        this.parsedRows = parsedRows;
        this.config = config;
        this.status = status;
    }

    @Override
    public void run() {

    }
}
