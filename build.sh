BUILD_PATH=$PWD

mkdir ./bin
VEDA_BIN=$BUILD_PATH/bin

if [ $1 == "exim-inquire" ] || [ $1 == "veda-exim-inquire" ] || [ $1 == "exim" ] || [ -z $1 ]; then
    echo BUILD EXIM-INQUIRE
    rm ./veda-exim-inquire

    cd veda-exim-inquire
    cargo build --release
    cd $BUILD_PATH
    cp $CARGO_TARGET_DIR/release/veda-exim-inquire $VEDA_BIN

fi

if [ $1 == "exim-respond" ] || [ $1 == "veda-exim-respond" ] || [ $1 == "exim" ] || [ -z $1 ]; then
    echo BUILD EXIM-RESPOND

    rm ./veda-exim-respond
    cd veda-exim-respond

    cargo +nightly build --release

    cd $BUILD_PATH
    cp $CARGO_TARGET_DIR/release/veda-exim-respond $VEDA_BIN

fi

if [ $1 == "extractor" ] || [ $1 == "veda-extractor" ] || [ $1 == "exim" ] || [ -z $1 ]; then
    echo BUILD VEDA-EXTRACTOR
    rm ./veda-extractor

    cd veda-extractor
    cargo build --release
    cd $BUILD_PATH
    cp $CARGO_TARGET_DIR/release/veda-extractor $VEDA_BIN
fi

