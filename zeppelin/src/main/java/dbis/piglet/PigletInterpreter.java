package dbis.piglet;

import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.spark.SparkInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
// import dbis.pig.PigCompiler;

public class PigletInterpreter extends SparkInterpreter {
    Logger logger = LoggerFactory.getLogger(PigletInterpreter.class);

    static {
        Interpreter.register("piglet", PigletInterpreter.class.getName());
    }

    public PigletInterpreter(Properties property) {
        super(property);
        logger.info("PigletInterpreter created");
    }

    public void open() {
        logger.info("PigletInterpreter.open");
        super.open();
    }

    public void close() {
        logger.info("PigletInterpreter.close");
        super.close();
    }

    public InterpreterResult interpret(String line, InterpreterContext context) {
        if (line == null || line.trim().length() == 0) {
            return new InterpreterResult(Code.SUCCESS);
        }
        return super.interpret(line, context);
        /*
        String[] params = { "--backend", "spark", "--master", "local[2]" };
        PigCompiler.main(params);
        InterpreterResult result = new InterpreterResult(Code.SUCCESS, "%tableCol1\tCol2\nAAA\t42\nBBB\t14\nCCC\t18");

        return result;
        */
    }
}
