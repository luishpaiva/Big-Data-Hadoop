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
public class Informacao7 {
    
    public static class MapperInformacao7 extends Mapper<Object, Text, Text, LongWritable> {
        
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            
            String linha = valor.toString();
            String[] campos = linha.split(";");
            long pesoMercadoria = 0;
            
            if (campos.length == 10 && !"".equals(campos[6])) {
                String mercadoria = campos[3];

                try {
                    pesoMercadoria = Long.parseLong(campos[6]);
                } catch (NumberFormatException ex) {
                    System.out.println("Exceção encontrada: " + ex);
                } 
                

                Text chaveMap = new Text(mercadoria);
                LongWritable valorMap = new LongWritable(pesoMercadoria);
                
                context.write(chaveMap, valorMap);
            }
            
        }
        
    }
    
    public static class ReducerInformacao7 extends Reducer<Text, LongWritable, Text, LongWritable> {
        
        int somaPesoMercadoria = 0;
        Text maiorMercadoria = new Text();

        @Override
        public void reduce(Text chave, Iterable<LongWritable> valores, Context context) throws IOException, InterruptedException {
            
            int soma = 0;
            
            for (LongWritable val : valores) {
                soma += val.get();
            }
            
            if (soma > somaPesoMercadoria) {
                somaPesoMercadoria = soma;
                maiorMercadoria.set(chave);
            }

        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(maiorMercadoria, new LongWritable(somaPesoMercadoria));
        }
        
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String caminhoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String caminhoSaida = "/home2/ead2022/SEM1/paiva.l/Desktop/luis.paiva/informacao7";
        
        if (args.length == 2) {
            caminhoEntrada = args[0];
            caminhoSaida = args[1];
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "informacao7Resposta");
        
        job.setJarByClass(Informacao1.class);
        job.setMapperClass(MapperInformacao7.class);
        job.setReducerClass(ReducerInformacao7.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(caminhoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(caminhoSaida));
        
        job.waitForCompletion(true);
    }
    
}
