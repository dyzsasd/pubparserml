package com.multiposting.pubparserml.clean;

/**
 * Created by shuai on 11/5/14.
 */
public class test {
    public static void main(String[] args){
        String text = "\\nLes Unités de Pilotage Réseau ont pour missions de piloter les investissements nécessaires au développement des réseaux fixes et mobiles, de mettre en œuvre la production des réseaux, et d’améliorer la qualité de service fournie à nos clients. Le service « Outils et Système d’Information » a en charge le développement et le maintien des applications locales du Système d’Information technique sur le réseau télécom et recherche actuellement un Ingénieur Développeur.";
        System.out.println(text.split("\t").length);
    }
}