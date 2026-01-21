#!/bin/bash

echo "================================================"
echo "  Learning Mode - Explore Scala 3 Features"
echo "================================================"
echo ""
echo "This mode runs examples without external services"
echo "Perfect for exploring Scala 3 features and ETL"
echo ""

# Menu
echo "Choose what you'd like to do:"
echo ""
echo "1. Compile all modules"
echo "2. Run Spark ETL locally"
echo "3. Explore Preprocessing module (Scala 3)"
echo "4. Run tests"
echo "5. Start interactive Scala REPL"
echo ""
read -p "Enter choice (1-5): " choice

case $choice in
    1)
        echo ""
        echo "📦 Compiling all modules..."
        sbt compile
        echo "✅ Compilation complete!"
        ;;
    2)
        echo ""
        echo "⚡ Running Spark ETL locally..."
        echo "   (Reads test data, runs aggregations)"
        sbt "etl/run"
        ;;
    3)
        echo ""
        echo "🎓 Exploring Preprocessing Module (Scala 3)"
        echo ""
        echo "Key Scala 3 features in this module:"
        echo "  • Enums with parameters"
        echo "  • Opaque types for type safety"
        echo "  • Extension methods"
        echo "  • Given/using context parameters"
        echo ""
        read -p "View the code? (y/n) " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo ""
            echo "Opening Person.scala (main Scala 3 features)..."
            less preprocessing/src/main/scala/preprocessing/models/Person.scala
        fi
        ;;
    4)
        echo ""
        echo "🧪 Running all tests..."
        sbt test
        ;;
    5)
        echo ""
        echo "🐚 Starting Scala REPL for preprocessing module..."
        echo "   Try this:"
        echo "   > import preprocessing.models.*"
        echo "   > val person = Person.create(\"Alice\", \"alice@example.com\", 25, \"NYC\")"
        echo "   > person.map(_.toSummary)"
        echo ""
        sbt "preprocessing/console"
        ;;
    *)
        echo "Invalid choice"
        exit 1
        ;;
esac

echo ""
echo "================================================"
echo "  Learning Resources"
echo "================================================"
echo ""
echo "📚 Documentation:"
echo "   • README.md      - Project overview"
echo "   • CLAUDE.md      - Architecture & technical details"
echo ""
echo "📂 Key Files to Explore:"
echo "   • preprocessing/src/main/scala/preprocessing/models/Person.scala"
echo "     (Scala 3 features: enums, opaque types, extensions)"
echo ""
echo "   • etl/src/main/scala/etl/SparkETLPipeline.scala"
echo "     (Spark aggregations & transformations)"
echo ""
echo "   • shared/src/main/scala/shared/config/Paths.scala"
echo "     (Shared configuration across modules)"
echo ""
echo "🔗 External Resources:"
echo "   • Scala 3 Handbook: https://docs.scala-lang.org/scala3/"
echo "   • Apache Spark Docs: https://spark.apache.org/docs/latest/"
echo "   • ZIO Docs: https://zio.dev/"
echo ""
