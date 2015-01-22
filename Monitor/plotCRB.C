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
void plotCRB(int type=1, float interval=1)
{
  TString  styleMacro = gSystem->Getenv("MIT_ROOT_STYLE");
  long int rc = gROOT->LoadMacro(styleMacro+"+");
  printf(" Return code of loading styles: %d\n",rc);
  plotDatasetUsage(type,interval);
}

//--------------------------------------------------------------------------------------------------
void plotDatasetUsage(int type=1, float interval=1.)
{
  TString text        = gSystem->Getenv("DATASET_MONITOR_TEXT");
  TString fileName    = gSystem->Getenv("DATASET_MONITOR_FILE");
  TString prorate     = gSystem->Getenv("PRORATE_REPLICAS");
  TString byReplicas  = gSystem->Getenv("FILL_BY_REPLICAS");
  TString deleted     = gSystem->Getenv("INCLUDE_DELETED");

  TString pngFileName = fileName + TString(".png");
  pngFileName = fileName + TString(".png"); // if we're using nSitesAv instead of nSites
  TString inputFile   = fileName + TString(".txt");

  // Make sure we have the right styles
  MitRootStyle::Init();

  // Now open our database output
  ifstream input;
  input.open(inputFile.Data());

  Int_t    nLines=0;
  Double_t totalSize=0;
  Int_t    nFiles=0, nAccesses=0;
  Int_t isOld=1, isDeleted=0;
  Double_t size=0;
  Double_t time=0;
  Int_t nSites=0;
  TString  name, site;


  // book our histogram
  TH1D *h;
  TH1D *h2 ; // h is used for plotting, h2 is for computing some stuff
  h = new TH1D("dataUsage","Data Usage",18,-2.5,15.5);
  MitRootStyle::InitHist(h,"","",kBlack);
  //TString titles = TString("; Accesses ") + text + TString(";Data Size [TB]");
  //if(interval!=1) titles = TString("; Accesses/day ") + TString(";Data Size [TB]");
  TString titles = TString("; nAccesses ") +  TString("; Data Volume [TB]");
  // printf("%f %s\n",interval,titles.Data());   exit(-1);
  h->SetTitle(titles.Data());
  // Loop over the file
  //-------------------
  TAxis *xaxis = h->GetXaxis();
  xaxis->SetBinLabel(1,"nAcc=0 (Older than x)");
  xaxis->SetBinLabel(2,"nAcc=0 (Newer than x)");
  xaxis->SetBinLabel(3,"0");
  xaxis->SetBinLabel(4,"1");
  xaxis->SetBinLabel(18,">14");
  Double_t deletedSize=0;
  while (1) {
    // read in
    //input >> nSites >> nAccesses >> nFiles >> size;
    if (byReplicas == "yes") {
      input >> nAccesses >> nFiles >> size >> time  >> name >> site >> isOld >> isDeleted;
    } else {
      input >> nAccesses >> nFiles >> size >> nSites  >> name >>  isOld >> isDeleted;
    }
    // printf("%d %d %f %s %d %d\n",nAccesses,nFiles,size,name.Data(),isOld,input.fail());
    // check it worked
    if (! input.good())
      break;

    Double_t value, fillvalue, weight;
    if (prorate == "yes") value = double(nAccesses)/double(nFiles); 
    else {
      if (byReplicas == "no") value = double(nAccesses)/double(nFiles*nSites);
      else value = double(nAccesses)/double(nFiles);
    }
    if (byReplicas == "yes") weight = double(size*time)/1000.;
    else weight = double(nSites)*size/1000.;

    if (value>=14)
      fillvalue = 15;
    
    // fill the histogram here
    if (isDeleted==1 && deleted=="no") continue;
    if (isDeleted==1) deletedSize+=size;
    if (nAccesses==0) {
      if (isOld==1) fillvalue=-2;
      else fillvalue=-1;
    } else if (value < 1) {
      fillvalue = 1;
    } else if (value > 14.5) {
      fillvalue = 15;
    } else {
      fillvalue = value;
    }

    h->Fill(fillvalue,weight);
    if (fillvalue == 0) {
      printf(" Value: %f filled: %f weight: %f\n",value,fillvalue,weight);
    }

    // keep track of the total size
    else totalSize += size/1000.;

    // count the number of lines
    nLines++;
  }
  input.close();

  printf(" \n");
  printf(" Found %d entries.\n",nLines);
  printf(" Found %.3f [PB] total volume.\n",totalSize/1000.);
  printf(" Unused fraction this period: %.3f.\n",(h->GetBinContent(1)+h->GetBinContent(2))/h->Integral());
  printf(" Deleted volume: %.3f [PB]\n",deletedSize/1000000.);
  printf(" \n");

  // Open a canvas
  TCanvas *cv = new TCanvas("c","c",1200,600);
 // if (type==2) cv->SetLogy();
  cv->Draw();
  gPad->SetBottomMargin(0.15);
  Double_t integral=h->Integral();
  // h->Scale(1/integral);
  // h->SetMaximum(0.3); // for easy comparison between plots
  Double_t maxy = h->GetMaximum();
  h->SetMaximum(maxy*1.1); 
  if (text == "CRB_3MONTHS") {
    h->SetLineColor(kBlue);
  } else if (text == "CRB_6MONTHS") {
    h->SetLineColor(kRed);
  } else if (text == "CRB_12MONTHS") {
    h->SetLineColor(kGreen);
  } 
  h->Draw("hist");
  TFile *fout = new TFile("CRBPlots_"+prorate+"_"+byReplicas+"_"+deleted+".root","UPDATE");
  fout->cd();
  h->Write(fileName);

  // MitRootStyle::OverlayFrame();
  // MitRootStyle::AddText("Overflow in last bin. nAccesses=0 in first bin.");

  cv->SaveAs(pngFileName.Data());
}
