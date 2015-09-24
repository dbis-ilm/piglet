package dbis.piglet;

import org.apache.zeppelin.interpreter.*;
import java.util.*;

public class PigletInterpreter extends Interpreter {
	public PigletInterpreter(Properties property) {
        super(property);
	}

	public void open() {
	}

	public void close() {
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

	public InterpreterResult interpret(String st, InterpreterContext context) {
        return null;
	}
}
