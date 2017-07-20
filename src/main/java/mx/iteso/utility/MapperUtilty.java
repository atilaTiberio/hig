package mx.iteso.utility;

/**
 * Created by hiturbe on 17/07/17.
 */
public class MapperUtilty {


    public static String mapImc(String imcRaw){
        switch (imcRaw){
            case "1":
                imcRaw="Normal";
                break;
            case "2":
                imcRaw="Obesidad";
                break;
            case "3":
                imcRaw="Sobrepeso";
                break;
            default:
                imcRaw="ND";
                break;

        }
        return imcRaw;
    }
}
