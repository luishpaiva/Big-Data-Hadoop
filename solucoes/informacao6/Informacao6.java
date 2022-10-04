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
public class Informacao6 {
    
    public static class MapperInformacao6 extends Mapper<Object, Text, Text, LongWritable> {
        
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            
            String linha = valor.toString();
            String[] campos = linha.split(";");
            long valorMercadoria = 0;
            
            if (campos.length == 10 && campos[1].equals("2016") && campos[0].equals("Brazil")) {
                
                try {
                
                    String mercadoria = campos[3];
                    valorMercadoria = Long.parseLong(campos[5]);

                    Text chaveMap = new Text(mercadoria);
                    LongWritable valorMap = new LongWritable(valorMercadoria);
                
                    context.write(chaveMap, valorMap);
                } catch (IOException | InterruptedException | NumberFormatException erro) {
                    System.out.println("Erro encontrado: " + erro);
                }
            }
            
        }
        
    }
    
    public static class ReducerInformacao6 extends Reducer<Text, LongWritable, Text, LongWritable> {
        
        int somaValorMercadoria = 0;
        Text maiorMercadoria = new Text();

        @Override
        public void reduce(Text chave, Iterable<LongWritable> valores, Context context) throws IOException, InterruptedException {
            
            int soma = 0;
            
            for (LongWritable val : valores) {
                soma += val.get();
            }
            
            if (soma > somaValorMercadoria) {
                somaValorMercadoria = soma;
                maiorMercadoria.set(chave);
            }

        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(maiorMercadoria, new LongWritable(somaValorMercadoria));
        }
        
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String caminhoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String caminhoSaida = "/home2/ead2022/SEM1/paiva.l/Desktop/luis.paiva/informacao6";
        
        if (args.length == 2) {
            caminhoEntrada = args[0];
            caminhoSaida = args[1];
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "informacao6Resposta");
        
        job.setJarByClass(Informacao1.class);
        job.setMapperClass(MapperInformacao6.class);
        job.setReducerClass(ReducerInformacao6.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(caminhoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(caminhoSaida));
        
        job.waitForCompletion(true);
    }
    
}
