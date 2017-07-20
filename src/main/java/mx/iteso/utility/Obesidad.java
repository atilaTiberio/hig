package mx.iteso.utility;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by hiturbe on 19/07/17.
 */
public class Obesidad {

    /*
    El mapper genera esta tipo
    si la reduccion fue por entidad o bien por entidad y genero
     */
    private String tipo;

    private String entidad;
    private String genero;
    /*
    Normal, obesidad, sobrepeso
     */
    private String clase;

    private String total;

    public Obesidad(String[] registro){
        /*
                0 tipo
                1 entidad
                2 genero ( sera vacio para el tipo entidad_genero
                3 clase (normal, obesidad, sobrepeso)
                4 total no sera necesario hacer un Integer.parseInt
         */

        this.tipo=registro[0];
        this.entidad=registro[1];

        if(this.tipo.equals("entidad_genero")) {
            this.genero = registro[2];
            this.clase=registro[3];
            this.total=registro[4];
        }
        else {
            this.clase = registro[2];
            this.total=registro[3];
        }

    }

    public Map<String, AttributeValue> getDynamoItem(){
        Map<String, AttributeValue> item= new HashMap<String, AttributeValue>();

        item.put("tipo", new AttributeValue(this.tipo));
        item.put("entidad", new AttributeValue(this.entidad));
        item.put("clase",new AttributeValue(this.clase));
        item.put("total",new AttributeValue(this.total));

       if(this.tipo!=null&&this.tipo.equals("entidad_genero"))
           item.put("genero",new AttributeValue(this.genero));


        SimpleDateFormat sdf= new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS_");
        Random r = new Random();

        String time=sdf.format(new Date(System.currentTimeMillis()))+r.nextInt(Integer.MAX_VALUE);
        AttributeValue ts= new AttributeValue();
        ts.setS(time);
        item.put("timestamp",ts);


        return item;

    }

    @Override
    public String toString() {
        return "Obesidad{" +
                "tipo='" + tipo + '\'' +
                ", entidad='" + entidad + '\'' +
                ", genero='" + genero + '\'' +
                ", clase='" + clase + '\'' +
                ", total='" + total + '\'' +
                '}';
    }
}
