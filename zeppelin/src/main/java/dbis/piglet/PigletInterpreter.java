package dbis.piglet;

import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;

import org.apache.spark.repl.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

import dbis.piglet.api.PigletInterpreterAPI;

import scala.tools.nsc.settings.MutableSettings.BooleanSetting;

public class PigletInterpreter extends Interpreter {
    Logger logger = LoggerFactory.getLogger(PigletInterpreter.class);
    SparkILoop iLoop = null;
    scala.tools.nsc.Settings settings = new scala.tools.nsc.Settings();

    BufferedReader input = null;
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    static {
        Interpreter.register("piglet", PigletInterpreter.class.getName());
    }

    public PigletInterpreter(Properties property) {
        super(property);
        logger.info("PigletInterpreter created");
    }

    public void open() {
        logger.info("PigletInterpreter.open");
//        org.apache.spark.repl.Main.classServer().start();
        input = new BufferedReader(new StringReader(""));
        iLoop = new SparkILoop(input, new PrintWriter(out));
        BooleanSetting b = (BooleanSetting) settings.usejavacp();
        b.v_$eq(true);
        settings.scala$tools$nsc$settings$StandardScalaSettings$_setter_$usejavacp_$eq(b);
    }

    public void close() {
        logger.info("PigletInterpreter.close");
        iLoop.closeInterpreter();
//        org.apache.spark.repl.Main.classServer().stop();
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
        
        // TODO: add language feature COMPLETE here
        List<String> features = new LinkedList<String>();
        features.add("CompletePiglet");
        String sparkCode = PigletInterpreterAPI.createCodeFromInput(line, "spark", features);
        sparkCode += "\nsc.stop()";
        // sparkCode = "println(\"%table x\ty\\n1\t2\\n3\t4\\n\\n\")";
        logger.info("PigletInterpreter.interpret = " + sparkCode);
        String res = iLoop.run(sparkCode, settings);
        // System.out.println("PigletInterpreter.interpret = '" + sparkCode + "'");
       // if (res.matches(".*<console>:\\d+: error:.*")) {
        if (res.contains("error:") || res.contains("Exception:")) {
            // System.out.println("ERROR: " + res);
            logger.error("ERROR in interpreter: " + res);
            InterpreterResult result = new InterpreterResult(Code.ERROR, "");
            return result;
        }
        else {
            // System.out.println("HUHUUUUUUU");
            logger.info("result = " + res);
            InterpreterResult result = new InterpreterResult(Code.SUCCESS, extractResult(res));
            return result;
        }
    }

    private String extractResult(String output) {
        int pos = output.indexOf("%table ");
        if (pos == -1)
            return output;
        int last = output.indexOf("\n\n", pos);
        String extracted = output.substring(pos, last);
        logger.info("extracted result = {" + extracted + "}");
        return extracted;
    }
}
