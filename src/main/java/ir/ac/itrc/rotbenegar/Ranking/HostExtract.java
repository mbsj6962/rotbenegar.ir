package ir.ac.itrc.rotbenegar.Ranking;

import com.google.common.net.InternetDomainName;

public class HostExtract {

    /**
     * Will take a url such as http://www.stackoverflow.com and return
     * www.stackoverflow.com
     *
     * @param url
     * @return
     * @throws Exception
     */
    public static String getHost(String urlString) throws Exception {

        String url=urlString;
        url = url.trim().replace("\n", "").replace("\r", "").toLowerCase();

        int doubleslash = url.indexOf("//");
        if (doubleslash == -1) {
            doubleslash = 0;
        } else {
            doubleslash += 2;
        }

        int slash = url.indexOf('/', doubleslash);
        if (slash == -1) {
            slash = url.length();
        }

        int space = url.indexOf(' ', doubleslash);
        if (space == -1) {
            space = url.length();
        }

        int questionMark = url.indexOf('?', doubleslash);
        if (questionMark == -1) {
            questionMark = url.length();
        }

        int numberSign = url.indexOf('#', doubleslash);
        if (numberSign == -1) {
            numberSign = url.length();
        }

        int ampersand = url.indexOf('&', doubleslash);
        if (ampersand == -1) {
            ampersand = url.length();
        }

        int colonSign = url.indexOf(':', doubleslash);
        if (colonSign == -1) {
            colonSign = url.length();
        }

        int end = slash < space ? slash : space;
        end = end < questionMark ? end : questionMark;
        end = end < numberSign ? end : numberSign;
        end = end < ampersand ? end : ampersand;
        end = end < colonSign ? end : colonSign;

       String host = url.substring(doubleslash, end);

        if (host.startsWith("www.") && (host.length() > 4)) {
            host = host.substring(4);

        }
        String[] domainNames = host.split("\\.");
        if (domainNames.length <= 2) {
            return host;
        } else {
            // The domain name part should not start or end with dash (-) (e.g. -google-.com)
            Boolean badname= false;
            for (int index=0; index<domainNames.length; index++)
                if (domainNames[index].startsWith("-")|| domainNames[index].startsWith("_") || domainNames[index].endsWith("-") || domainNames[index].endsWith("_"))
                    badname=true;
            if (badname) return "INVALID_DOMAIN";
            try {
                InternetDomainName internatDomainName = InternetDomainName.from(host);

                if (internatDomainName.isUnderPublicSuffix()) {
                    return internatDomainName.topPrivateDomain().toString().replace("InternetDomainName{name=", "").replace("}", "");
                } else {
                    return "INVALID_DOMAIN";
                }
            } catch (Exception e) {
                //check if it is a number
                Boolean flag = true;
                for (int i = 0; i < domainNames.length; i++) {
                    flag = flag & isInteger(domainNames[i]);
                }
                if (domainNames.length == 4) {
                    if (Integer.parseInt(domainNames[0]) < 256 && Integer.parseInt(domainNames[1]) < 256 && Integer.parseInt(domainNames[2]) < 256 && Integer.parseInt(domainNames[3]) < 256) {
                        return host;
                    }
                }
                throw e;
            }
        }

    }

    public static boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return false;
        } catch (NullPointerException e) {
            return false;
        }
        // only got here if we didn't return false
        return true;
    }
}