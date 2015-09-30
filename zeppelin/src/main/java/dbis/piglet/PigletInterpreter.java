package dbis.piglet;

import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import org.apache.spark.repl.*;
import dbis.pig.PigCompiler;
import scala.tools.nsc.settings.*;
import scala.tools.nsc.settings.MutableSettings.BooleanSetting;

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
        org.apache.spark.repl.Main.classServer().start();
    }

    public void close() {
        logger.info("PigletInterpreter.close");
        org.apache.spark.repl.Main.classServer().stop();
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
        logger.info("generated code = " + sparkCode);
        BufferedReader input = new BufferedReader(new StringReader(sparkCode));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        SparkILoop iLoop = new SparkILoop(input, new PrintWriter(out));
        scala.tools.nsc.Settings settings = new scala.tools.nsc.Settings();
        BooleanSetting b = (BooleanSetting) settings.usejavacp();
        b.v_$eq(true);
        settings.scala$tools$nsc$settings$StandardScalaSettings$_setter_$usejavacp_$eq(b);
        // iLoop.initializeSpark();

        // boolean res = iLoop.process(settings);
        String res = iLoop.run(sparkCode, settings);
        logger.info("result = " + res);
        InterpreterResult result = new InterpreterResult(Code.SUCCESS, out.toString());
        iLoop.closeInterpreter();
        return result;
    }
}
