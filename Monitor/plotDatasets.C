//==================================================================================================
//
// Plot dataset characteristics as reported in the local popularity database cache.
//
//
//==================================================================================================
#include <iostream>
#include <fstream>

#include <TROOT.h>
#include <TSystem.h>
#include <TStyle.h>
#include <TString.h>
#include <TCanvas.h>
#include <TGraph.h>
#include <TMultiGraph.h>
#include <TH1D.h>
#include <TLegend.h>
#include <TText.h>
using namespace std;

void plotDatasetUsage();

//--------------------------------------------------------------------------------------------------
void plotDatasets(int type=0, float interval=1)
{
  TString  styleMacro = gSystem->Getenv("MIT_ROOT_STYLE");
  long int rc = gROOT->LoadMacro(styleMacro+"+");
  printf(" Return code of loading styles: %d\n",rc);

  plotDatasetUsage(type,interval);
}

//--------------------------------------------------------------------------------------------------
void plotDatasetUsage(int type=0, float interval=1.)
{
  TString text        = gSystem->Getenv("DATASET_MONITOR_TEXT");
  TString fileName    = gSystem->Getenv("DATASET_MONITOR_FILE");

  TString pngFileName = fileName + TString(".png");
  if(type==1) pngFileName = fileName + TString("_nSitesAverage.png"); // if we're using nSitesAv instead of nSites
  else pngFileName = fileName + TString("_nSites.png"); // if we're using nSitesAv instead of nSites
  TString inputFile   = fileName + TString(".txt");

  // Make sure we have the right styles
  MitRootStyle::Init();

  // Now open our database output
  ifstream input;
  input.open(inputFile.Data());

  Int_t    nLines=0;
  Double_t totalSize=0;
  Double_t xMin=0, xMax=120.;
  Double_t yMin=0, yMax=10;
  Int_t    nSites=0, nFiles=0, nAccesses=0;
  Double_t nSitesAv, size=0;
  TString  name;

  // book our histogram
  Int_t nBins = int(xMax-xMin);
  if (interval!=1.) xMax = xMax/14.; // divide by approximate number of months
  Int_t nBinsy = 20;
  TH1D *h;
  TH1D *h2 ; // h is used for plotting, h2 is for computing some stuff
  if (interval==1.) h = new TH1D("dataUsage","Data Usage",nBins,xMin-0.5,xMax+0.5);
  else h = new TH1D("dataUsage","Data Usage",nBins+1,xMin-(0.5+1)/14.,xMax+0.5/14.);
  if (interval==1.) h2 = new TH1D("dataUsage2","Data Usage",nBins,xMin-0.5,xMax+0.5);
  else h2 = new TH1D("dataUsage2","Data Usage",nBins,xMin-(0.5)/14.,xMax+0.5/14.);
  MitRootStyle::InitHist(h,"","",kBlack);
  //TString titles = TString("; Accesses ") + text + TString(";Data Size [TB]");
  //if(interval!=1) titles = TString("; Accesses/day ") + TString(";Data Size [TB]");
  TString titles = TString("; Accesses ") + text + TString("; Fraction of total data volume");
  if(interval!=1.) titles = TString("; Accesses/month ") + TString("; Fraction of total data volume");
  h->SetTitle(titles.Data());
  if(interval!=1.)  interval=interval/(86400*30);
  // Loop over the file
  //-------------------
  while (1) {

    // read in
    //input >> nSites >> nAccesses >> nFiles >> size;
    input >> nSites >> nSitesAv >> nAccesses >> nFiles >> size >> name;

    // check it worked
    if (! input.good())
      break;
    
    if (type==1) {
      // do we want nSites or nSitesAv
      // show what we are reading
      if (nLines < 5)
        printf(" nSitesAv=%.3f  nAccesses=%d  nFiles=%d  size=%8f interval=%.3f\n",nSitesAv, nAccesses, nFiles, size,interval);
    } else {
      // show what we are reading
      if (nLines < 5)
        printf(" nSites=%d  nAccesses=%d  nFiles=%d  size=%8f\n",nSites, nAccesses, nFiles, size);
    }
    Double_t value, valuey, weight;
    
    if (type==1) {
      value = double(nAccesses)/double(nFiles*nSitesAv*interval); 
      weight = double(nSitesAv)*size/1000.;
    } else {  
      value = double(nAccesses)/double(nFiles*nSites*interval); 
      weight = double(nSites)*size/1000.;
    }

    // treat the cases of few accesses:
    //
    // - to avoid confusing counts below zero pretend that accessing half of the files of
    //   one replica counts as one full access
    //   not desired behavior when plotting nAccesses/GB, usually < 1
    // if ( value<1.0 && (double(nAccesses)/double(nFiles)>0.5))
    //   value = 1.;

    // truncate at maximum (overflow in last bin)

    if (value>=xMax)
      value = xMax-0.0001;
    
    // fill the histogram here
    if (value==0) h->Fill(h->GetBinCenter(1),weight);
    else h->Fill(value,weight);
    h2->Fill(value,weight);
    if (nLines < 5) {
      if(type==2) printf(" Value: %f,%f weight: %f\n",value,valuey,weight);
      else printf(" Value: %f weight: %f\n",value,weight);
    }

    // keep track of the total size
    if (type) totalSize += double(nSitesAv)*size/1000.;
    else totalSize += double(nSites)*size/1000.;

    // count the number of lines
    nLines++;
  }
  input.close();

  printf(" \n");
  printf(" Found %d entries.\n",nLines);
  printf(" Found %.3f [PB] total volume.\n",totalSize/1000.);
  printf(" \n");

  // Open a canvas
  TCanvas *cv = new TCanvas();
 // if (type==2) cv->SetLogy();
  cv->Draw();
  Double_t integral=h->Integral();
  h->Scale(1/integral);
  // h->SetMaximum(0.3); // for easy comparison between plots
  Double_t maxy = h->GetMaximum();
  h->SetMaximum(maxy*1.1); 
  h->Draw("hist");

  MitRootStyle::OverlayFrame();
  MitRootStyle::AddText("Overflow in last bin. nAccesses=0 in first bin.");
  TString integralText = "Data managed: ";
  char buffer[32];
  sprintf(buffer,"%.3f PB",totalSize/1000.);
  integralText+=buffer;
  TText *plotText = new TText(.4,.75,integralText.Data());
  plotText->SetTextSize(0.04);
  plotText->Draw();
  TText *plotText2 = new TText(.4,.8,text.Data());
  plotText2->SetTextSize(0.04);
  plotText2->Draw();
  text = "Mean: ";
  sprintf(buffer,"%.3f accesses/month",h2->GetMean(1));
  text += buffer;
  TText *plotText3 = new TText(.4,.7,text.Data());
  plotText3->SetTextSize(0.04);
  plotText3->Draw();
  cv->SaveAs(pngFileName.Data());
}
