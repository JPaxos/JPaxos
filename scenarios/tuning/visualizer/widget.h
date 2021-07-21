#pragma once

#include <QtDataVisualization/Q3DSurface>
#include <QtWidgets>

using namespace QtDataVisualization;

class Widget : public QWidget {
public:
    Widget(QFile && dataFile, QWidget *parent = nullptr);
    ~Widget();

protected:
    void initData(QFile & dataFile);

    void updateGraph();
    
    void closeEvent(QCloseEvent *event);

    // model -> reqsize -> ws -> bs -> mbps
    QMap<QString,QMap<int,QMap<int, QMap<int,double>>>> data;

    Q3DSurface * graph;
    QSurfaceDataProxy * proxy;
    QSurface3DSeries * series;
    QLayout * l;
    QComboBox * models;
    QComboBox * requestSizes;
    QSpinBox * bsTics;
    QSpinBox * wsTics;
};
