package dbis.piglet;

import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import org.apache.spark.repl.*;
import dbis.pig.PigCompiler;

public class PigletInterpreter extends Interpreter {
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
    }

    public void close() {
        logger.info("PigletInterpreter.close");
    }

    public void cancel(InterpreterContext context) {
    }

    public FormType getFormType() {
        return FormType.SIMPLE;
    }

    public int getProgress(InterpreterContext context) {
        return 100;
    }

    public List<String> completion(String buf, int cursor) {
        return new ArrayList<String>();
    }

    public InterpreterResult interpret(String line, InterpreterContext context) {
        if (line == null || line.trim().length() == 0) {
            return new InterpreterResult(Code.SUCCESS);
        }
        String sparkCode = PigCompiler.createCodeFromInput(line, "spark");
        BufferedReader input = new BufferedReader(new StringReader(sparkCode));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SparkILoop iLoop = new SparkILoop(input, new PrintWriter(out));
        scala.tools.nsc.Settings settings = new scala.tools.nsc.Settings();
        boolean res = iLoop.process(settings);
        InterpreterResult result = new InterpreterResult(Code.SUCCESS, "Hallo");

        return result;
    }
}
