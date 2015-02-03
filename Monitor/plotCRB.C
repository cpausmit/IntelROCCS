//==================================================================================================
//
// Plot dataset characteristics as reported in the local popularity database cache.
//
//
//==================================================================================================
#include <iostream>
#include <fstream>

#include <MitRootStyle.C>

#include <TROOT.h>
#include <TSystem.h>
#include <TFile.h>
#include <TStyle.h>
#include <TString.h>
#include <TCanvas.h>
#include <TGraph.h>
#include <TMultiGraph.h>
#include <TH1D.h>
#include <TLegend.h>
#include <TText.h>
using namespace std;

void plotDatasetUsage(int type=1, float interval=1., int debug=0);
Color_t getColor(TString text);
void setHistColor(TH1* h, TString text);
void printHistogram(TH1* h, TString text, ofstream &o);

//--------------------------------------------------------------------------------------------------
void plotCRB(int type=1, float interval=1)
{
  plotDatasetUsage(type,interval);
}

//--------------------------------------------------------------------------------------------------
void plotDatasetUsage(int type, float interval, int debug)
{
  TString text        = gSystem->Getenv("DATASET_MONITOR_TEXT");
  TString fileName    = gSystem->Getenv("DATASET_MONITOR_FILE");
  TString prorate     = gSystem->Getenv("PRORATE_REPLICAS");
  TString byReplicas  = gSystem->Getenv("FILL_BY_REPLICAS");
  TString deleted     = gSystem->Getenv("INCLUDE_DELETED");

  TString pngFileName = fileName + TString(".png");
  TString inputFile   = fileName + TString(".txt");
  TString outputFile  = fileName + TString(".dat");
  printf(" Interval?: %f\n",interval);
  printf(" Inputfile: %s\n",inputFile.Data());

  bool lProrate    = (prorate == "yes");
  bool lByReplicas = (byReplicas == "yes");
  bool lDeleted    = (deleted == "yes");

  // Make sure we have the right styles
  MitRootStyle::Init(-1);

  // Now open our database output
  ifstream input;
  ofstream outp;
  input.open(inputFile.Data());
  outp.open(outputFile.Data());

  Int_t    nDeleted = 0;
  Int_t    nLines = 0;
  Double_t totalSize = 0;
  Int_t    nFiles = 0, nAccesses = 0;
  Int_t    isOld = 1, isDeleted = 0;
  Double_t size = 0;
  Double_t time = 0;
  Int_t    nSites = 0;
  TString  name, site;
  TString  titles;

  // Book our histogram(s)
  TH1D *h = new TH1D("dataUsage","Data Usage",17,-1.5,15.5);
  MitRootStyle::InitHist(h,"","",kBlack);
  titles = TString("; n_{Accesses}") +  TString("; Data Volume [TB]");
  h->SetTitle(titles.Data());

  TH1D *hZeroOne = new TH1D("bin0","Zero and One Bin",100,0,1.);
  MitRootStyle::InitHist(hZeroOne,"","",kBlack);
  titles = TString("; <n_{Accesses}>") +  TString("; Data Volume [TB]");
  hZeroOne->SetTitle(titles.Data());

  TH1D *hTime = new TH1D("time","time",100,0,1.);
  MitRootStyle::InitHist(hTime,"","",kBlack);
  titles = TString("; Time Prorating") +  TString("; Data Volume [TB]");
  hTime->SetTitle(titles.Data());

  // custom axis labels for nAccess plot
  TAxis *xaxis = h->GetXaxis();
  xaxis->SetBinLabel( 1,"0 old");
  xaxis->SetBinLabel( 2,"0 fresh");
  xaxis->SetBinLabel( 3,"1");
  xaxis->SetBinLabel( 4,"2");
  xaxis->SetBinLabel( 5,"3");
  xaxis->SetBinLabel( 6,"4");
  xaxis->SetBinLabel( 7,"5");
  xaxis->SetBinLabel( 8,"6");
  xaxis->SetBinLabel( 9,"7");
  xaxis->SetBinLabel(10,"8");
  xaxis->SetBinLabel(11,"9");
  xaxis->SetBinLabel(12,"10");
  xaxis->SetBinLabel(13,"11");
  xaxis->SetBinLabel(14,"12");
  xaxis->SetBinLabel(15,"13");
  xaxis->SetBinLabel(16,"14");
  xaxis->SetBinLabel(17,">14");

  // Loop over the file
  //-------------------
  Double_t deletedSize = 0;
  while (1) {
    if (lByReplicas)
      input >> nAccesses >> nFiles >> size >> time >> name >> site >> isOld >> isDeleted;
    else
      input >> nAccesses >> nFiles >> size >> nSites  >> name >> isOld >> isDeleted;

    // check that it worked
    if (! input.good())
      break;

    // Always keep track of how many deleted datasets were there
    if (isDeleted == 1) {
      nDeleted++;
      deletedSize += size;
    }
    
    // Stop processing if this is a deleted dataset and they are not supposed to be shown
    if (isDeleted == 1 && (! lDeleted))
      continue;

    // Debugging print out
    if (debug>0)
      printf("%d %d %f %d %d\n",nAccesses,nFiles,size,isOld,input.fail());

    // Determine the x-axis value -> value and the fill height -> weight that should be filled into
    // the histogram, also special cases modify the value -> fillValue

    Double_t value, fillValue, weight;

    // start with weight -- the easy part
    if (lByReplicas)
      weight = double(size*time)/1000.;
    else
      weight = double(nSites)*size/1000.;

    // next find the value -- careful here
    if (lProrate)
      value = double(nAccesses)/double(nFiles); 
    else {
      if (lByReplicas)
	value = double(nAccesses)/double(nFiles);
      else
	value = double(nAccesses)/double(nFiles*nSites);
    }

    // finally deal with special cases: fillValue
    if      (nAccesses == 0 && isOld)
      fillValue = -1;
    else if (nAccesses == 0)
      fillValue = 0;
    else if (value < 0.5)
      fillValue = 0;
    else if (value < 1)
      fillValue = 1;
    else if (value > 14)
      fillValue = 15;
    else
      fillValue = value;

    // fill the histogram
    h->Fill(fillValue,weight);
    // and keep track of the total datasize
    totalSize += size/1000.;
    
    // show distribution of the values between zero and one
    if (fillValue == 0 || fillValue == 1)
      hZeroOne->Fill(value,weight);

    // time prorating is recorded with the size of dataset
    hTime->Fill(time,size/1000.);

    // debug: say how many lines every 1000 lines
    if (debug>0 && nLines%1000 == 1)
      printf(" progress .. %d\n",nLines);

    // count the number of lines
    nLines++;
  }
  input.close();

  // Give a summary report of what we have analyzed
  printf(" \n");
  printf(" Found %d entries.\n",nLines);
  printf(" Found %.3f [PB] total volume.\n",totalSize/1000.);
  printf(" Found %d deleted datasets.\n",nDeleted);
  printf(" Deleted volume: %.3f [PB]\n",deletedSize/1000000.);
  printf(" Unused fraction this period: %.3f.\n",
	 (h->GetBinContent(1)+h->GetBinContent(2))/h->Integral());
  printf(" \n");

  // Open a canvas
  TCanvas *cv = new TCanvas("c","c",800,600);
  if (type == 2)
    cv->SetLogy();
  cv->Draw();

  gPad->SetBottomMargin(0.20);
  Double_t maxy = h->GetMaximum();
  h->SetMaximum(maxy*1.1); 
  h->SetTitleOffset(1.400,"X");

  // Finally choose color and draw the usage histogram
  setHistColor(h,text);
  h->Draw("hist");
  printHistogram(h,text,outp);
  outp.close();
  MitRootStyle::OverlayFrame();
  TText *txt = MitRootStyle::AddText(text,0.70,0.90);
  txt->SetTextSize(0.04);
  txt->SetTextColor(getColor(text));
 
  // Make a png file of the canvas
  cv->SaveAs(fileName+".png");
  delete cv;

  // Open a canvas
  cv = new TCanvas("c","c",700,600);
  cv->Draw();
  // Draw the histogram
  setHistColor(hZeroOne,text);
  hZeroOne->Draw("hist");
  MitRootStyle::OverlayFrame();
  MitRootStyle::AddText("Cases of 0 to 1 accesses.");
  // Make a png file of the canvas
  cv->SaveAs(fileName+"_ZeroOneBin.png");
  delete cv;

  // Open a canvas
  cv = new TCanvas("c","c",700,600);
  cv->Draw();
  // Draw the histogram
  setHistColor(hTime,text);
  hTime->Draw("hist");
  MitRootStyle::OverlayFrame();
  MitRootStyle::AddText("Time pro-rating per volume.");
  // Make a png file of the canvas
  cv->SaveAs(fileName+"_Time.png");
  delete cv;

  //// Store it into a root file
  //TFile *fout = new TFile("CRBPlots_"+prorate+"_"+byReplicas+"_"+deleted+".root","UPDATE");
  //fout->cd();
  //h->Write(fileName);
  //hZeroOne->Write(fileName);
  //hTime->Write(fileName);
}

Color_t getColor(TString text)
{
  Color_t color = kBlack;
  if      (text == "CRB_3MONTHS" || text == "03M")
    color = kBlue;
  else if (text == "CRB_6MONTHS" || text == "06M")
    color = kRed;
  else if (text == "CRB_12MONTHS" || text == "12M")
    color = kGreen;
  return color;
}

void setHistColor(TH1* h, TString text)
{
  if      (text == "CRB_3MONTHS" || text == "03M")
    h->SetLineColor(kBlue);
  else if (text == "CRB_6MONTHS" || text == "06M")
    h->SetLineColor(kRed);
  else if (text == "CRB_12MONTHS" || text == "12M")
    h->SetLineColor(kGreen);
}

void printHistogram(TH1* h, TString text, ofstream &o)
{
  int nBins = h->GetNbinsX();
  o << text << ",";
  for (int iBin=1; iBin<=nBins; iBin++) {
    //printf("%.2f,\n",h->GetBinContent(iBin));
    o << h->GetBinContent(iBin) << ",";
  }
  o << endl;
}
