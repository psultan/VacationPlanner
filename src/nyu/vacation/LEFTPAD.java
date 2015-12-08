package nyu.vacation;

//modified version from http://stackoverflow.com/questions/26932995/left-padding-a-string-in-pig/26938187#26938187
//leftpad (padlength, chararray)


import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class LEFTPAD extends EvalFunc<String> {
@Override
public String exec(Tuple arg0) throws IOException {
       try
        {
            int pad = ((int)arg0.get(0));
            String input = ((String) arg0.get(1));
            return StringUtils.leftPad(input, pad, "0");
        }
        catch(Exception e)
        {
            throw new IOException("Caught exception while processing the input row ", e);
        }
    }
}
