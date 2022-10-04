package com.pucpr.implementacaomr;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class Informacao1 {
    
    public static class MapperInformacao1 extends Mapper<Object, Text, Text, IntWritable> {
        
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            
            String linha = valor.toString();
            String[] campos = linha.split(";");
            
            if (campos.length == 10) {
                String pais = campos[0];
                int ocorrencia = 1;
                
                Text chaveMap = new Text(pais);
                IntWritable valorMap = new IntWritable(ocorrencia);
                
                context.write(chaveMap, valorMap);
            }
            
        }
        
    }
    
    public static class ReducerInformacao1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        int maior = 0;
        Text paisMaior = new Text();

        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException {
            
            int soma = 0;
            
            for (IntWritable val : valores) {
                soma += val.get();
            }
            
            if (soma > maior) {
                maior = soma;
                paisMaior.set(chave);
            }

        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(paisMaior, new IntWritable(maior));
        }
        
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
        String caminhoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String caminhoSaida = "/home2/ead2022/SEM1/paiva.l/Desktop/luis.paiva/informacao1";
        
        if (args.length == 2) {
            caminhoEntrada = args[0];
            caminhoSaida = args[1];
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "informacao1Resposta");
        
        job.setJarByClass(Informacao1.class);
        job.setMapperClass(MapperInformacao1.class);
        job.setReducerClass(ReducerInformacao1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(caminhoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(caminhoSaida));
        
        job.waitForCompletion(true);
    }
    
}
