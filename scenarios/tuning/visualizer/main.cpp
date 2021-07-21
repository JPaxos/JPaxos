#include "widget.h"

#include <QApplication>

int main(int argc, char *argv[])
{
    QApplication a(argc, argv);
    QString file = QFileDialog::getOpenFileName();
    if(file.isNull())
        return 0;
    QFile data(file);
    data.open(QFile::ReadOnly);
    if(!data.isOpen()){
        QMessageBox::critical(nullptr, "Error", "Cannot open file " + file + ": " + data.errorString() );
        return 1;
    }
    Widget w(std::move(data));
    w.show();
    return a.exec();
}
