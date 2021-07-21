#include "widget.h"

struct MyFormatter : public QValue3DAxisFormatter{
    MyFormatter(QObject *parent = nullptr) : QValue3DAxisFormatter(parent) {}
    ~MyFormatter(){}
    QString stringForValue(qreal value, const QString &) const override {
        for(int i : std::array<int, 3>{-1,0,1})
            if(0==(((int)value)+i)%1024)
                 return QString::number((int)(value-1)/1024)+"k";
         return QString::number(value/1024)+"k";
    }
};

Widget::Widget(QFile && dataFile, QWidget *parent)
    : QWidget(parent)
{
    graph = new Q3DSurface();
    proxy = new QSurfaceDataProxy(graph);
    series = new QSurface3DSeries(graph);
    series->setDataProxy(proxy);

    l = new QVBoxLayout;
    l->addWidget(QWidget::createWindowContainer(graph, this));
    setLayout(l);

    QLayout *l2 = new QHBoxLayout;

    models = new QComboBox(this);
    l2->addWidget(models);

    requestSizes = new QComboBox(this);
    l2->addWidget(requestSizes);

    QRadioButton * slNo = new QRadioButton("——", this);
    QRadioButton * slBs = new QRadioButton("BS", this);
    QRadioButton * slWs = new QRadioButton("WS", this);
    slNo->setChecked(true);
    connect(slNo, &QRadioButton::toggled, [&](bool on){if(on) graph->setSelectionMode(Q3DSurface::SelectionItem);});
    connect(slBs, &QRadioButton::toggled, [&](bool on){if(on) graph->setSelectionMode(Q3DSurface::SelectionItemAndRow|Q3DSurface::SelectionSlice);});
    connect(slWs, &QRadioButton::toggled, [&](bool on){if(on) graph->setSelectionMode(Q3DSurface::SelectionItemAndColumn|Q3DSurface::SelectionSlice);});

    QCheckBox * rBs = new QCheckBox("Rev BS", this);
    QCheckBox * rWs = new QCheckBox("Rev WS", this);
    connect(rBs, &QCheckBox::toggled, [&](bool on){graph->axisX()->setReversed(on);});
    connect(rWs, &QCheckBox::toggled, [&](bool on){graph->axisZ()->setReversed(on);});

    bsTics = new QSpinBox(this);
    wsTics = new QSpinBox(this);
    bsTics->setMinimum(1);
    wsTics->setMinimum(1);
    void (QSpinBox::*spinValChngd)(int) = &QSpinBox::valueChanged;
    connect(bsTics, spinValChngd, [&](int v){graph->axisX()->setSegmentCount(v);});
    connect(wsTics, spinValChngd, [&](int v){graph->axisZ()->setSegmentCount(v);});

    QPushButton * openFile = new QPushButton("Open...", this);
    connect(openFile, &QPushButton::clicked, [&]{
        QString file = QFileDialog::getOpenFileName(this);
        if(file.isNull())
            return;
        QFile data(file);
        data.open(QFile::ReadOnly);
        if(!data.isOpen()){
            QMessageBox::critical(this, "Error", "Cannot open file " + file + ": " + data.errorString() );
            return;
        }
        initData(data);
    });

    for(auto w: std::array<QWidget*, 10>{new QLabel("BS tics:", this), bsTics,
        new QLabel("WS tics:", this), wsTics, slNo, slBs, slWs, rBs, rWs, openFile}){
        l2->addWidget(w);
        w->setMaximumWidth(w->minimumSizeHint().width());
    }

    l->addItem(l2);
    setGeometry(geometry().x(), geometry().y(), 800, 600);

    initData(dataFile);
    updateGraph();

    connect(models,       &QComboBox::currentTextChanged, this, &Widget::updateGraph);
    connect(requestSizes, &QComboBox::currentTextChanged, this, &Widget::updateGraph);

    QLinearGradient gr;
    gr.setColorAt(0.0, Qt::black);
    gr.setColorAt(0.33, Qt::blue);
    gr.setColorAt(0.67, Qt::red);
    gr.setColorAt(1.0, Qt::yellow);

    series->setBaseGradient(gr);
    series->setColorStyle(Q3DTheme::ColorStyleRangeGradient);


    graph->axisX()->setTitle("Batching size");
    graph->axisX()->setTitleVisible(true);
    graph->axisY()->setTitle("MB/s");
    graph->axisY()->setTitleVisible(true);
    graph->axisZ()->setTitle("Window size");
    graph->axisZ()->setTitleVisible(true);

    graph->axisX()->setFormatter(new MyFormatter(this));

    graph->setHorizontalAspectRatio(1);
    graph->addSeries(series);
}

void Widget::updateGraph(){
    QString model = models->currentText();
    int reqSize = requestSizes->currentText().toInt();
    if(model.isEmpty()||reqSize==0)
        return;

    const auto & currentData = data[model][reqSize];
    QSurfaceDataArray *dataArray = new QSurfaceDataArray;
    for(auto it = currentData.begin(); it !=currentData.end(); ++it){
        QSurfaceDataRow *row = new QSurfaceDataRow();
        for(auto it2 = it.value().begin(); it2 !=it.value().end(); ++it2)
            row->append(QSurfaceDataItem(QVector3D(it2.key(), it2.value(), it.key())));
        dataArray->append(row);
    }
    proxy->resetArray(dataArray);
}

void Widget::initData(QFile &dataFile){
    // header
    dataFile.readLine();

    QFileInfo fi(dataFile);
    setWindowTitle(fi.fileName() + " (" + fi.path() + ")");

    data.clear();
    while(true){
        auto bytes = dataFile.readLine();
        if(bytes.isEmpty()) break;
        QString line = QString::fromLocal8Bit(bytes);
        QStringList entries = line.split(',');

        if(entries.count() < 8){
            QMessageBox::critical(this, "Error", "Bad data: " + line);
            QApplication::exit(1);
        }

        // model -> reqsize -> ws -> bs -> mbps
        data[entries[4]][entries[2].toInt()][entries[0].toInt()][entries[1].toInt()]=
                (9.+entries[2].toInt())/2/1024/1024*entries[7].toDouble();
    }
    dataFile.close();

    models->clear();
    requestSizes->clear();
    models->addItems(data.keys());
    for(auto s : data.first().keys())
        requestSizes->addItem(QString::number(s));
    int cntBS = data.first().first().first().count()-1;
    int cntWS = data.first().first().lastKey()-data.first().first().firstKey();
    bsTics->setValue(cntBS);
    wsTics->setValue(cntWS);
}

void Widget::closeEvent(QCloseEvent * e){
    QWidget::closeEvent(e);
    QApplication::exit(0);
}

Widget::~Widget() {
    delete graph;
}
