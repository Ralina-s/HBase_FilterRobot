import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class RobotsFilter {

    List<Pattern> patterns = new ArrayList<Pattern>();

    public RobotsFilter() {

    }

    public RobotsFilter(String rules_together) throws BadFormatException{
        String [] rules = rules_together.split("\\n");

        for (String rule: rules) {
            if (rule.startsWith("Disallow:")) {
                String rule_subst = rule.substring(("Disallow:").length()).trim();
                if (rule_subst.startsWith("/")) {
                    patterns.add(Pattern.compile("^" + rule_subst.replaceAll("\\.", "\\\\.")));
                } else if (rule_subst.startsWith("*")) {
                    patterns.add(Pattern.compile(rule_subst.substring(1).replaceAll("\\.", "\\\\.")));
                } else {
                    patterns.add(Pattern.compile(rule_subst.replaceAll("\\.", "\\\\.")));
                }
            } else  if (rule.length() != 0){
                throw new BadFormatException();
            }
        }
    }

    public class BadFormatException extends Exception{
        public BadFormatException(){}
    }


    boolean IsAllowed(String checkString) {

        for (Pattern rule: patterns) {
            if (rule.matcher(checkString).find()) {
                return false;
            }
        }
        return true;
    }
}
