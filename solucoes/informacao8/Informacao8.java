package com.pucpr.implementacaomr;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author paiva.l
 */
public class Informacao8 {
    
    public static class MapperInformacao8 extends Mapper<Object, Text, Text, LongWritable> {

        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            
            String[] campos = valor.toString().split(";");
                
            if (campos.length == 10 && !"".equals(campos[6])) {
                try {
                    
                    Text chaveMap = new Text(campos[3] + "   " + campos[1]);
                    LongWritable valorMap = new LongWritable(Long.parseLong(campos[6]));

                    context.write(chaveMap, valorMap);
                } catch (IOException | InterruptedException | NumberFormatException erro) {
                    System.out.println("Erro encontrado: " + erro);
                }
                
            }

        }

    }
    
    public static class ReducerInformacao8 extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text chave, Iterable<LongWritable> valor, Context context) throws IOException, InterruptedException {
            
            long maior = 0;

            for (LongWritable valores : valor) {
                if (valores.get() > maior) {
                    maior = valores.get();
                }

            }

            context.write(chave, new LongWritable(maior));

        }

    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String caminhoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String caminhoSaida = "/home2/ead2022/SEM1/paiva.l/Desktop/luis.paiva/informacao8";
        
        if (args.length == 2) {
            caminhoEntrada = args[0];
            caminhoSaida = args[1];
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "informacao8Resposta");
        
        job.setJarByClass(Informacao1.class);
        job.setMapperClass(MapperInformacao8.class);
        job.setReducerClass(ReducerInformacao8.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(caminhoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(caminhoSaida));
        
        job.waitForCompletion(true);
    }
    
}
