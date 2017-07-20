package mx.iteso.mapper;

import com.opencsv.CSVParser;

import mx.iteso.utility.MapperUtilty;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;





/**
 * Created by hiturbe on 14/06/17.
 */
public class MapperCSV extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        CSVParser parser= new CSVParser();
        String[] parts=parser.parseLine(value.toString());
        Text[] detalle= obesidadRecord(parts);
        context.write(detalle[0], new IntWritable(1));
        context.write(detalle[1], new IntWritable(1));

        }


    private Text[] obesidadRecord(String[] parts){


        String entidad=parts[2]; //este campo viene como 01 AGUASCALIENTES, tengo que transformar para remover la clave de entidad
        String sexo= parts[4];
        String imcClass=parts[13];
        entidad=entidad.replaceFirst("\\d*\\s","");

        if(sexo.contains("1"))
            sexo="Mujer";
        if(sexo.contains("2"))
            sexo="Hombre";

       imcClass= MapperUtilty.mapImc(imcClass);

        StringBuffer sb= new StringBuffer();


        sb.append("entidad_genero,");
        sb.append(entidad);
        sb.append(",");
        sb.append(sexo);
        sb.append(",");
        sb.append(imcClass);


        Text estadoGenero= new Text(sb.toString());
        sb.replace(0,sb.length(),"");

        sb.append("entidad,");
        sb.append(entidad);
        sb.append(",");
        sb.append(imcClass);


        Text estado= new Text(sb.toString());
        sb.replace(0,sb.length(),"");

        return new Text[]{estadoGenero,estado};

    }



}
